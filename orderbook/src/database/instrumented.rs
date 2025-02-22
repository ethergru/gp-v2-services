use super::{orders::OrderStoring, trades::TradeRetrieving, Postgres};
use crate::fee::MinFeeStoring;
use ethcontract::H256;
use model::order::Order;
use prometheus::Histogram;
use shared::{event_handling::EventStoring, maintenance::Maintaining};
use std::sync::Arc;

// The pool uses an Arc internally.
#[derive(Clone)]
pub struct Instrumented {
    inner: Postgres,
    metrics: Arc<dyn Metrics>,
}

impl Instrumented {
    pub fn new(inner: Postgres, metrics: Arc<dyn Metrics>) -> Self {
        Self { inner, metrics }
    }
}

pub trait Metrics: Send + Sync {
    fn database_query_histogram(&self, label: &str) -> Histogram;
}

#[async_trait::async_trait]
impl EventStoring<contracts::gpv2_settlement::Event> for Instrumented {
    async fn replace_events(
        &mut self,
        events: Vec<ethcontract::Event<contracts::gpv2_settlement::Event>>,
        range: std::ops::RangeInclusive<shared::event_handling::BlockNumber>,
    ) -> anyhow::Result<()> {
        let _timer = self
            .metrics
            .database_query_histogram("replace_events")
            .start_timer();
        self.inner.replace_events(events, range).await
    }

    async fn append_events(
        &mut self,
        events: Vec<ethcontract::Event<contracts::gpv2_settlement::Event>>,
    ) -> anyhow::Result<()> {
        let _timer = self
            .metrics
            .database_query_histogram("append_events")
            .start_timer();
        self.inner.append_events(events).await
    }

    async fn last_event_block(&self) -> anyhow::Result<u64> {
        let _timer = self
            .metrics
            .database_query_histogram("last_event_block")
            .start_timer();
        self.inner.last_event_block().await
    }
}

#[async_trait::async_trait]
impl MinFeeStoring for Instrumented {
    async fn save_fee_measurement(
        &self,
        fee_data: crate::fee::FeeData,
        expiry: chrono::DateTime<chrono::Utc>,
        min_fee: ethcontract::U256,
    ) -> anyhow::Result<()> {
        let _timer = self
            .metrics
            .database_query_histogram("save_fee_measurement")
            .start_timer();
        self.inner
            .save_fee_measurement(fee_data, expiry, min_fee)
            .await
    }

    async fn find_measurement_exact(
        &self,
        fee_data: crate::fee::FeeData,
        min_expiry: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Option<ethcontract::U256>> {
        let _timer = self
            .metrics
            .database_query_histogram("find_measurement_exact")
            .start_timer();
        self.inner
            .find_measurement_exact(fee_data, min_expiry)
            .await
    }

    async fn find_measurement_including_larger_amount(
        &self,
        fee_data: crate::fee::FeeData,
        min_expiry: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Option<ethcontract::U256>> {
        let _timer = self
            .metrics
            .database_query_histogram("find_measurement_including_larger_amount")
            .start_timer();
        self.inner
            .find_measurement_including_larger_amount(fee_data, min_expiry)
            .await
    }
}

#[async_trait::async_trait]
impl OrderStoring for Instrumented {
    async fn insert_order(
        &self,
        order: &model::order::Order,
    ) -> anyhow::Result<(), super::orders::InsertionError> {
        let _timer = self
            .metrics
            .database_query_histogram("insert_order")
            .start_timer();
        self.inner.insert_order(order).await
    }

    async fn cancel_order(
        &self,
        order_uid: &model::order::OrderUid,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        let _timer = self
            .metrics
            .database_query_histogram("cancel_order")
            .start_timer();
        self.inner.cancel_order(order_uid, now).await
    }

    async fn orders(
        &self,
        filter: &super::orders::OrderFilter,
    ) -> anyhow::Result<Vec<model::order::Order>> {
        let _timer = self
            .metrics
            .database_query_histogram("orders")
            .start_timer();
        self.inner.orders(filter).await
    }

    async fn orders_for_tx(&self, tx_hash: &H256) -> anyhow::Result<Vec<Order>> {
        let _timer = self
            .metrics
            .database_query_histogram("orders_for_tx")
            .start_timer();
        self.inner.orders_for_tx(tx_hash).await
    }

    async fn single_order(
        &self,
        uid: &model::order::OrderUid,
    ) -> anyhow::Result<Option<model::order::Order>> {
        let _timer = self
            .metrics
            .database_query_histogram("single_order")
            .start_timer();
        self.inner.single_order(uid).await
    }

    async fn solvable_orders(
        &self,
        min_valid_to: u32,
    ) -> anyhow::Result<super::orders::SolvableOrders> {
        let _timer = self
            .metrics
            .database_query_histogram("solvable_orders")
            .start_timer();
        self.inner.solvable_orders(min_valid_to).await
    }

    async fn user_orders(
        &self,
        owner: &ethcontract::H160,
        offset: u64,
        limit: Option<u64>,
    ) -> anyhow::Result<Vec<model::order::Order>> {
        let _timer = self
            .metrics
            .database_query_histogram("user_orders")
            .start_timer();
        self.inner.user_orders(owner, offset, limit).await
    }
}

#[async_trait::async_trait]
impl TradeRetrieving for Instrumented {
    async fn trades(
        &self,
        filter: &super::trades::TradeFilter,
    ) -> anyhow::Result<Vec<model::trade::Trade>> {
        let _timer = self
            .metrics
            .database_query_histogram("trades")
            .start_timer();
        self.inner.trades(filter).await
    }
}

#[async_trait::async_trait]
impl Maintaining for Instrumented {
    async fn run_maintenance(&self) -> anyhow::Result<()> {
        let _timer = self
            .metrics
            .database_query_histogram("remove_expired_fee_measurements")
            .start_timer();
        self.inner.run_maintenance().await
    }
}
