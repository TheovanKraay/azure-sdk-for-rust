// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! End-to-end verification of the **read consistency strategy** wire contract.
//!
//! These tests drive the full driver pipeline against the in-memory emulator
//! and use a [`RequestObserver`] to inspect the outgoing
//! `x-ms-cosmos-read-consistency-strategy` header on each request the transport
//! actually sees. They prove that:
//!
//! 1. An explicitly requested strategy (`Eventual`, `Session`,
//!    `LatestCommitted`, `GlobalStrong`) is serialized on the wire with its
//!    canonical value, and the read still succeeds against the emulator.
//! 2. `ReadConsistencyStrategy::Default` (and an unset strategy) emit **no**
//!    header — `Default` means "inherit" from the account/client/environment.
//!
//! The Gateway applies the requested read consistency server-side, so on the
//! wire the SDK's only responsibility is to emit the header faithfully.

use std::sync::{Arc, Mutex};

use azure_core::http::{headers::HeaderName, Method, Request, Url};
use azure_data_cosmos_driver::{
    in_memory_emulator::{
        ConsistencyLevel, InMemoryEmulatorHttpClient, RequestObserver, VirtualAccountConfig,
        VirtualRegion,
    },
    models::{AccountReference, ContainerReference, CosmosOperation, ItemReference, PartitionKey},
    options::{DriverOptions, OperationOptionsBuilder, ReadConsistencyStrategy},
    CosmosDriver,
};
use serde::{Deserialize, Serialize};

const EMULATOR_GATEWAY_URL: &str = "https://eastus.emulator.local";
const EMULATOR_KEY: &str = "dGVzdGtleQ==";

static READ_CONSISTENCY_STRATEGY_HEADER: HeaderName =
    HeaderName::from_static("x-ms-cosmos-read-consistency-strategy");

#[derive(Debug, Serialize, Deserialize)]
struct TestItem {
    id: String,
    pk: String,
    value: i64,
}

/// Snapshot of a single request observed by the emulator, capturing the method,
/// URL, and outgoing `x-ms-cosmos-read-consistency-strategy` header (if any).
#[derive(Clone, Debug)]
struct RequestSnapshot {
    method: Method,
    url: Url,
    read_consistency_strategy: Option<String>,
}

impl RequestSnapshot {
    /// Returns `true` when this request targets the item data-plane path
    /// `dbs/{db}/colls/{coll}/docs[/{id}]`.
    fn is_item_request(&self) -> bool {
        let mut segments = match self.url.path_segments() {
            Some(s) => s,
            None => return false,
        };
        segments.next() == Some("dbs")
            && segments.next().is_some()
            && segments.next() == Some("colls")
            && segments.next().is_some()
            && segments.next() == Some("docs")
    }

    /// Returns `true` for a point read (`GET`) of an item.
    fn is_item_read(&self) -> bool {
        self.method == Method::Get && self.is_item_request()
    }
}

/// [`RequestObserver`] that records every request the emulator sees so tests can
/// assert on the outgoing read consistency strategy header.
#[derive(Debug, Default)]
struct RecordingObserver {
    snapshots: Mutex<Vec<RequestSnapshot>>,
}

impl RecordingObserver {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn snapshots(&self) -> Vec<RequestSnapshot> {
        self.snapshots
            .lock()
            .expect("recording observer lock poisoned")
            .clone()
    }

    fn clear(&self) {
        self.snapshots
            .lock()
            .expect("recording observer lock poisoned")
            .clear();
    }

    /// Returns the single item read recorded since the last [`Self::clear`],
    /// panicking if there is not exactly one.
    fn single_item_read(&self) -> RequestSnapshot {
        let reads: Vec<RequestSnapshot> = self
            .snapshots()
            .into_iter()
            .filter(RequestSnapshot::is_item_read)
            .collect();
        assert_eq!(
            reads.len(),
            1,
            "expected exactly one item read, observed: {reads:#?}"
        );
        reads.into_iter().next().unwrap()
    }
}

impl RequestObserver for RecordingObserver {
    fn on_request(&self, request: &Request) {
        let snapshot = RequestSnapshot {
            method: request.method(),
            url: request.url().clone(),
            read_consistency_strategy: request
                .headers()
                .get_optional_str(&READ_CONSISTENCY_STRATEGY_HEADER)
                .map(|s| s.to_owned()),
        };
        self.snapshots
            .lock()
            .expect("recording observer lock poisoned")
            .push(snapshot);
    }
}

/// Test harness: an emulator wired to a recording observer plus a driver and a
/// resolved container, all under Session consistency (the account default).
struct Harness {
    driver: Arc<CosmosDriver>,
    observer: Arc<RecordingObserver>,
    container: ContainerReference,
}

impl Harness {
    async fn setup() -> Self {
        let observer = RecordingObserver::new();

        let config = VirtualAccountConfig::new(vec![VirtualRegion::new(
            "East US",
            Url::parse(EMULATOR_GATEWAY_URL).unwrap(),
        )])
        .unwrap()
        .with_consistency(ConsistencyLevel::Session);

        let emulator = Arc::new(
            InMemoryEmulatorHttpClient::new(config).with_request_observer(observer.clone()),
        );

        let db_name = "rcs_db";
        let container_name = "rcs_coll";
        let store = emulator.store();
        store.create_database(db_name);
        store.create_container(
            db_name,
            container_name,
            serde_json::from_value(serde_json::json!({
                "paths": ["/pk"],
                "kind": "Hash",
                "version": 2,
            }))
            .unwrap(),
        );

        let runtime = emulator.runtime_builder().build().await.unwrap();
        let account = AccountReference::with_master_key(
            Url::parse(EMULATOR_GATEWAY_URL).unwrap(),
            EMULATOR_KEY,
        );
        let driver = runtime
            .create_driver(DriverOptions::builder(account).build())
            .await
            .unwrap();

        let container = driver
            .resolve_container(db_name, container_name)
            .await
            .unwrap();

        Self {
            driver,
            observer,
            container,
        }
    }

    fn item_ref(&self, pk: &str, id: &str) -> ItemReference {
        ItemReference::from_name(
            &self.container,
            PartitionKey::from(pk.to_string()),
            id.to_string(),
        )
    }

    /// Creates an item so subsequent reads have something to return.
    async fn create(&self, pk: &str, id: &str, value: i64) {
        let body = serde_json::to_vec(&TestItem {
            id: id.to_string(),
            pk: pk.to_string(),
            value,
        })
        .unwrap();
        self.driver
            .execute_singleton_operation(
                CosmosOperation::create_item(self.item_ref(pk, id)).with_body(body),
                OperationOptionsBuilder::new().build(),
            )
            .await
            .expect("create_item should succeed");
    }

    /// Reads an item with the given (optional) read consistency strategy and
    /// returns the outgoing header value observed on the wire.
    async fn read_with_strategy(
        &self,
        pk: &str,
        id: &str,
        strategy: Option<ReadConsistencyStrategy>,
    ) -> Option<String> {
        self.observer.clear();
        let mut builder = OperationOptionsBuilder::new();
        if let Some(strategy) = strategy {
            builder = builder.with_read_consistency_strategy(strategy);
        }
        self.driver
            .execute_singleton_operation(
                CosmosOperation::read_item(self.item_ref(pk, id)),
                builder.build(),
            )
            .await
            .expect("read_item should succeed");
        self.observer.single_item_read().read_consistency_strategy
    }
}

/// A read issued with `ReadConsistencyStrategy::LatestCommitted` must carry
/// `x-ms-cosmos-read-consistency-strategy: LatestCommitted` on the wire, and
/// the read must still succeed against the emulator.
#[tokio::test]
async fn latest_committed_read_emits_header() {
    let h = Harness::setup().await;
    h.create("pk1", "item-1", 1).await;

    let observed = h
        .read_with_strategy(
            "pk1",
            "item-1",
            Some(ReadConsistencyStrategy::LatestCommitted),
        )
        .await;

    assert_eq!(
        observed.as_deref(),
        Some("LatestCommitted"),
        "a LatestCommitted read must carry the read-consistency-strategy header"
    );
}

/// Every explicitly requested non-Default strategy is serialized with its
/// canonical wire value.
#[tokio::test]
async fn explicit_strategies_emit_canonical_values() {
    let h = Harness::setup().await;
    h.create("pk1", "item-1", 1).await;

    for (strategy, expected) in [
        (ReadConsistencyStrategy::Eventual, "Eventual"),
        (ReadConsistencyStrategy::Session, "Session"),
        (ReadConsistencyStrategy::LatestCommitted, "LatestCommitted"),
        (ReadConsistencyStrategy::GlobalStrong, "GlobalStrong"),
    ] {
        let observed = h.read_with_strategy("pk1", "item-1", Some(strategy)).await;
        assert_eq!(
            observed.as_deref(),
            Some(expected),
            "strategy {strategy:?} must serialize as {expected:?}"
        );
    }
}

/// `ReadConsistencyStrategy::Default` means "inherit" and must emit no header.
#[tokio::test]
async fn default_strategy_omits_header() {
    let h = Harness::setup().await;
    h.create("pk1", "item-1", 1).await;

    let observed = h
        .read_with_strategy("pk1", "item-1", Some(ReadConsistencyStrategy::Default))
        .await;

    assert_eq!(
        observed, None,
        "a Default read must not carry the read-consistency-strategy header"
    );
}

/// A read with no strategy set must emit no header.
#[tokio::test]
async fn unset_strategy_omits_header() {
    let h = Harness::setup().await;
    h.create("pk1", "item-1", 1).await;

    let observed = h.read_with_strategy("pk1", "item-1", None).await;

    assert_eq!(
        observed, None,
        "a read with no strategy must not carry the read-consistency-strategy header"
    );
}
