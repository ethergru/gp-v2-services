use crate::{
    account_balances::{BalanceFetching, Query},
    database::orders::OrderStoring,
    orderbook::filter_unsupported_tokens,
};
use anyhow::Result;
use model::order::Order;
use primitive_types::U256;
use shared::{
    bad_token::BadTokenDetecting, current_block::CurrentBlockStream, time::now_in_epoch_seconds,
};
use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

/// Keeps track and updates the set of currently solvable orders.
/// For this we also need to keep track of user sell token balances for open orders so this is
/// retrievable as well.
pub struct SolvableOrdersCache {
    min_order_validity_period: Duration,
    database: Arc<dyn OrderStoring>,
    balance_fetcher: Arc<dyn BalanceFetching>,
    bad_token_detector: Arc<dyn BadTokenDetecting>,
    inner: Mutex<Inner>,
    notify: Notify,
}

type Balances = HashMap<Query, U256>;

struct Inner {
    orders: Vec<Order>,
    balances: Balances,
    block: u64,
    update_time: Instant,
}

impl SolvableOrdersCache {
    pub fn new(
        min_order_validity_period: Duration,
        database: Arc<dyn OrderStoring>,
        balance_fetcher: Arc<dyn BalanceFetching>,
        bad_token_detector: Arc<dyn BadTokenDetecting>,
        current_block: CurrentBlockStream,
    ) -> Arc<Self> {
        let self_ = Arc::new(Self {
            min_order_validity_period,
            database,
            balance_fetcher,
            bad_token_detector,
            inner: Mutex::new(Inner {
                orders: Default::default(),
                balances: Default::default(),
                block: 0,
                update_time: Instant::now(),
            }),
            notify: Default::default(),
        });
        tokio::task::spawn(update_task(Arc::downgrade(&self_), current_block));
        self_
    }

    pub fn cached_balance(&self, key: &Query) -> Option<U256> {
        let inner = self.inner.lock().unwrap();
        inner.balances.get(key).copied()
    }

    /// Orders and timestamp at which last update happened.
    pub fn cached_solvable_orders(&self) -> (Vec<Order>, Instant) {
        let inner = self.inner.lock().unwrap();
        (inner.orders.clone(), inner.update_time)
    }

    /// The cache will update the solvable orders and missing balances as soon as possible.
    pub fn request_update(&self) {
        self.notify.notify_one();
    }

    /// Manually update solvable orders. Usually called by the background updating task.
    pub async fn update(&self, block: u64) -> Result<()> {
        let min_valid_to = now_in_epoch_seconds() + self.min_order_validity_period.as_secs() as u32;
        let orders = self.database.solvable_orders(min_valid_to).await?;
        let orders = filter_unsupported_tokens(orders, self.bad_token_detector.as_ref()).await?;

        let old_balances = {
            let inner = self.inner.lock().unwrap();
            if inner.block == block {
                inner.balances.clone()
            } else {
                HashMap::new()
            }
        };
        let (mut new_balances, missing_queries) = new_balances(&old_balances, &orders);
        let fetched_balances = self.balance_fetcher.get_balances(&missing_queries).await;
        for (query, balance) in missing_queries.into_iter().zip(fetched_balances) {
            let balance = match balance {
                Ok(balance) => balance,
                Err(_) => continue,
            };
            new_balances.insert(query, balance);
        }

        let mut orders = solvable_orders(orders, &new_balances);
        for order in &mut orders {
            let query = Query::from_order(order);
            order.order_meta_data.available_balance = new_balances.get(&query).copied();
        }

        *self.inner.lock().unwrap() = Inner {
            orders,
            balances: new_balances,
            block,
            update_time: Instant::now(),
        };

        Ok(())
    }
}

/// Returns existing balances and Vec of queries that need to be peformed.
fn new_balances(old_balances: &Balances, orders: &[Order]) -> (HashMap<Query, U256>, Vec<Query>) {
    let mut new_balances = HashMap::new();
    let mut missing_queries = HashSet::new();
    for order in orders {
        let query = Query::from_order(order);
        match old_balances.get(&query) {
            Some(balance) => {
                new_balances.insert(query, *balance);
            }
            None => {
                missing_queries.insert(query);
            }
        }
    }
    let missing_queries = Vec::from_iter(missing_queries);
    (new_balances, missing_queries)
}

// The order book has to make a choice for which orders to include when a user has multiple orders
// selling the same token but not enough balance for all of them.
// Assumes balance fetcher is already tracking all balances.
fn solvable_orders(mut orders: Vec<Order>, balances: &Balances) -> Vec<Order> {
    let mut orders_map = HashMap::<Query, Vec<Order>>::new();
    orders.sort_by_key(|order| std::cmp::Reverse(order.order_meta_data.creation_date));
    for order in orders {
        let key = Query::from_order(&order);
        orders_map.entry(key).or_default().push(order);
    }

    let mut result = Vec::new();
    for (key, orders) in orders_map {
        let mut remaining_balance = match balances.get(&key) {
            Some(balance) => *balance,
            None => continue,
        };
        for order in orders {
            // TODO: This is overly pessimistic for partially filled orders where the needed balance
            // is lower. For partially fillable orders that cannot be fully filled because of the
            // balance we could also give them as much balance as possible instead of skipping. For
            // that we first need a way to communicate this to the solver. We could repurpose
            // availableBalance for this.
            let needed_balance = match order
                .order_creation
                .sell_amount
                .checked_add(order.order_creation.fee_amount)
            {
                Some(balance) => balance,
                None => continue,
            };
            if let Some(balance) = remaining_balance.checked_sub(needed_balance) {
                remaining_balance = balance;
                result.push(order);
            }
        }
    }
    result
}

/// Keep updating the cache when the current block changes or an update notification happens.
/// Exits when this becomes the only reference to the cache.
async fn update_task(cache: Weak<SolvableOrdersCache>, mut current_block: CurrentBlockStream) {
    loop {
        let cache = match cache.upgrade() {
            Some(self_) => self_,
            None => {
                tracing::debug!("exiting solvable orders update task");
                break;
            }
        };
        {
            let new_block = current_block.changed();
            let notified = cache.notify.notified();
            futures::pin_mut!(new_block);
            futures::pin_mut!(notified);
            futures::future::select(new_block, notified).await;
        }
        let block = match current_block.borrow().number {
            Some(block) => block.as_u64(),
            None => {
                tracing::error!("no block number");
                continue;
            }
        };
        match cache.update(block).await {
            Ok(()) => tracing::debug!("updated solvable orders"),
            Err(err) => tracing::error!(?err, "failed to update solvable orders"),
        }
    }
}
