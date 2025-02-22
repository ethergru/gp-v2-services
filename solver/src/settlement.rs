mod settlement_encoder;

pub use self::settlement_encoder::SettlementEncoder;
use crate::{
    encoding::{self, EncodedInteraction, EncodedSettlement, EncodedTrade},
    liquidity::Settleable,
};
use anyhow::Result;
use model::order::{Order, OrderKind};
use num::{BigRational, Signed, Zero};
use primitive_types::{H160, U256};
use shared::conversions::U256Ext as _;
use std::collections::HashMap;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Trade {
    pub order: Order,
    pub sell_token_index: usize,
    pub buy_token_index: usize,
    pub executed_amount: U256,
    pub scaled_fee_amount: U256,
    pub is_liquidity_order: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TradeExecution {
    pub sell_token: H160,
    pub buy_token: H160,
    pub sell_amount: U256,
    pub buy_amount: U256,
    pub fee_amount: U256,
}

impl Trade {
    // The difference between the minimum you were willing to buy/maximum you were willing to sell, and what you ended up buying/selling
    pub fn surplus(
        &self,
        sell_token_price: &BigRational,
        buy_token_price: &BigRational,
    ) -> Option<BigRational> {
        match self.order.order_creation.kind {
            model::order::OrderKind::Buy => buy_order_surplus(
                sell_token_price,
                buy_token_price,
                &self.order.order_creation.sell_amount.to_big_rational(),
                &self.order.order_creation.buy_amount.to_big_rational(),
                &self.executed_amount.to_big_rational(),
            ),
            model::order::OrderKind::Sell => sell_order_surplus(
                sell_token_price,
                buy_token_price,
                &self.order.order_creation.sell_amount.to_big_rational(),
                &self.order.order_creation.buy_amount.to_big_rational(),
                &self.executed_amount.to_big_rational(),
            ),
        }
    }

    // Returns the executed fee amount (prorated of executed amount)
    // cf. https://github.com/gnosis/gp-v2-contracts/blob/964f1eb76f366f652db7f4c2cb5ff9bfa26eb2cd/src/contracts/GPv2Settlement.sol#L370-L371
    pub fn executed_fee(&self) -> Option<U256> {
        self.compute_fee_execution(self.order.order_creation.fee_amount)
    }

    /// Returns the scaled unsubsidized fee amount that should be used for
    /// objective value computation.
    pub fn executed_scaled_unsubsidized_fee(&self) -> Option<U256> {
        self.compute_fee_execution(self.scaled_fee_amount)
    }

    fn compute_fee_execution(&self, fee_amount: U256) -> Option<U256> {
        match self.order.order_creation.kind {
            model::order::OrderKind::Buy => fee_amount
                .checked_mul(self.executed_amount)?
                .checked_div(self.order.order_creation.buy_amount),
            model::order::OrderKind::Sell => fee_amount
                .checked_mul(self.executed_amount)?
                .checked_div(self.order.order_creation.sell_amount),
        }
    }

    /// Computes and returns the executed trade amounts given sell and buy prices.
    pub fn executed_amounts(&self, sell_price: U256, buy_price: U256) -> Option<TradeExecution> {
        let order = &self.order.order_creation;
        let (sell_amount, buy_amount) = match order.kind {
            OrderKind::Sell => {
                let sell_amount = self.executed_amount;
                let buy_amount = sell_amount
                    .checked_mul(sell_price)?
                    .checked_ceil_div(&buy_price)?;
                (sell_amount, buy_amount)
            }
            OrderKind::Buy => {
                let buy_amount = self.executed_amount;
                let sell_amount = buy_amount.checked_mul(buy_price)?.checked_div(sell_price)?;
                (sell_amount, buy_amount)
            }
        };
        let fee_amount = self.executed_fee()?;

        Some(TradeExecution {
            sell_token: order.sell_token,
            buy_token: order.buy_token,
            sell_amount,
            buy_amount,
            fee_amount,
        })
    }

    /// Encodes the settlement trade as a tuple, as expected by the smart
    /// contract.
    pub fn encode(&self) -> EncodedTrade {
        encoding::encode_trade(
            &self.order.order_creation,
            self.sell_token_index,
            self.buy_token_index,
            &self.executed_amount,
        )
    }
}

pub trait Interaction: std::fmt::Debug + Send + Sync {
    // TODO: not sure if this should return a result.
    // Write::write returns a result but we know we write to a vector in memory so we know it will
    // never fail. Then the question becomes whether interactions should be allowed to fail encoding
    // for other reasons.
    fn encode(&self) -> Vec<EncodedInteraction>;
}

#[cfg(test)]
impl Interaction for EncodedInteraction {
    fn encode(&self) -> Vec<EncodedInteraction> {
        vec![self.clone()]
    }
}

#[cfg(test)]
#[derive(Debug)]
pub struct NoopInteraction;

#[cfg(test)]
impl Interaction for NoopInteraction {
    fn encode(&self) -> Vec<EncodedInteraction> {
        Vec::new()
    }
}

#[derive(Debug, Clone)]
pub struct Settlement {
    pub encoder: SettlementEncoder,
}

impl Settlement {
    /// Creates a new settlement builder for the specified clearing prices.
    pub fn new(clearing_prices: HashMap<H160, U256>) -> Self {
        Self {
            encoder: SettlementEncoder::new(clearing_prices),
        }
    }

    /// .
    pub fn with_liquidity<L>(&mut self, liquidity: &L, execution: L::Execution) -> Result<()>
    where
        L: Settleable,
    {
        liquidity
            .settlement_handling()
            .encode(execution, &mut self.encoder)
    }

    pub fn without_onchain_liquidity(&self) -> Self {
        let encoder = self.encoder.without_onchain_liquidity();
        Self { encoder }
    }

    #[cfg(test)]
    pub fn with_trades(clearing_prices: HashMap<H160, U256>, trades: Vec<Trade>) -> Self {
        let encoder = SettlementEncoder::with_trades(clearing_prices, trades);
        Self { encoder }
    }

    /// Returns the clearing prices map.
    pub fn clearing_prices(&self) -> &HashMap<H160, U256> {
        self.encoder.clearing_prices()
    }

    /// Returns the clearing price for the specified token.
    ///
    /// Returns `None` if the token is not part of the settlement.
    pub fn clearing_price(&self, token: H160) -> Option<U256> {
        self.clearing_prices().get(&token).copied()
    }

    /// Returns the currently encoded trades.
    pub fn trades(&self) -> &[Trade] {
        self.encoder.trades()
    }

    /// Returns an iterator of all executed trades.
    pub fn executed_trades(&self) -> impl Iterator<Item = TradeExecution> + '_ {
        self.trades()
            .iter()
            .map(move |trade| {
                let order = &trade.order.order_creation;
                trade.executed_amounts(
                    self.clearing_price(order.sell_token)?,
                    self.clearing_price(order.buy_token)?,
                )
            })
            .map(|execution| execution.expect("invalid trade was added to encoder"))
    }

    // Computes the total surplus of all protocol trades (in wei ETH).
    pub fn total_surplus(&self, external_prices: &HashMap<H160, BigRational>) -> BigRational {
        match self.encoder.total_surplus(external_prices) {
            Some(value) => value,
            None => {
                tracing::error!("Overflow computing objective value for: {:?}", self);
                num::zero()
            }
        }
    }

    // Computes the total scaled unsubsidized fee of all protocol trades (in wei ETH).
    pub fn total_scaled_unsubsidized_fees(
        &self,
        external_prices: &HashMap<H160, BigRational>,
    ) -> BigRational {
        self.encoder
            .trades()
            .iter()
            .filter(|trade| !trade.is_liquidity_order)
            .filter_map(|trade| {
                let fee_token_price =
                    external_prices.get(&trade.order.order_creation.sell_token)?;
                Some(trade.executed_scaled_unsubsidized_fee()?.to_big_rational() * fee_token_price)
            })
            .sum()
    }

    /// See SettlementEncoder::merge
    pub fn merge(self, other: Self) -> Result<Self> {
        let merged = self.encoder.merge(other.encoder)?;
        Ok(Self { encoder: merged })
    }
}

impl From<Settlement> for EncodedSettlement {
    fn from(settlement: Settlement) -> Self {
        settlement.encoder.finish()
    }
}

// The difference between what you were willing to sell (executed_amount * limit_price) converted into reference token (multiplied by buy_token_price)
// and what you had to sell denominated in the reference token (executed_amount * buy_token_price)
fn buy_order_surplus(
    sell_token_price: &BigRational,
    buy_token_price: &BigRational,
    sell_amount_limit: &BigRational,
    buy_amount_limit: &BigRational,
    executed_amount: &BigRational,
) -> Option<BigRational> {
    if buy_amount_limit.is_zero() {
        return None;
    }
    let res = executed_amount * sell_amount_limit / buy_amount_limit * sell_token_price
        - (executed_amount * buy_token_price);
    if res.is_negative() {
        None
    } else {
        Some(res)
    }
}

// The difference of your proceeds denominated in the reference token (executed_sell_amount * sell_token_price)
// and what you were minimally willing to receive in buy tokens (executed_sell_amount * limit_price)
// converted to amount in reference token at the effective price (multiplied by buy_token_price)
fn sell_order_surplus(
    sell_token_price: &BigRational,
    buy_token_price: &BigRational,
    sell_amount_limit: &BigRational,
    buy_amount_limit: &BigRational,
    executed_amount: &BigRational,
) -> Option<BigRational> {
    if sell_amount_limit.is_zero() {
        return None;
    }
    let res = executed_amount * sell_token_price
        - (executed_amount * buy_amount_limit / sell_amount_limit * buy_token_price);
    if res.is_negative() {
        None
    } else {
        Some(res)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::liquidity::SettlementHandling;
    use maplit::hashmap;
    use model::order::{OrderCreation, OrderKind};
    use num::FromPrimitive;
    use shared::addr;

    pub fn assert_settlement_encoded_with<L, S>(
        prices: HashMap<H160, U256>,
        handler: S,
        execution: L::Execution,
        exec: impl FnOnce(&mut SettlementEncoder),
    ) where
        L: Settleable,
        S: SettlementHandling<L>,
    {
        let actual_settlement = {
            let mut encoder = SettlementEncoder::new(prices.clone());
            handler.encode(execution, &mut encoder).unwrap();
            encoder.finish()
        };
        let expected_settlement = {
            let mut encoder = SettlementEncoder::new(prices);
            exec(&mut encoder);
            encoder.finish()
        };

        assert_eq!(actual_settlement, expected_settlement);
    }

    // Helper function to save some repetition below.
    fn r(u: u128) -> BigRational {
        BigRational::from_u128(u).unwrap()
    }

    /// Helper function for creating a settlement for the specified prices and
    /// trades for testing objective value computations.
    fn test_settlement(prices: HashMap<H160, U256>, trades: Vec<Trade>) -> Settlement {
        Settlement {
            encoder: SettlementEncoder::with_trades(prices, trades),
        }
    }

    #[test]
    fn sell_order_executed_amounts() {
        let trade = Trade {
            order: Order {
                order_creation: OrderCreation {
                    kind: OrderKind::Sell,
                    sell_amount: 10.into(),
                    buy_amount: 6.into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 5.into(),
            ..Default::default()
        };
        let sell_price = 3.into();
        let buy_price = 4.into();
        let execution = trade.executed_amounts(sell_price, buy_price).unwrap();

        assert_eq!(execution.sell_amount, 5.into());
        assert_eq!(execution.buy_amount, 4.into()); // round up!
    }

    #[test]
    fn buy_order_executed_amounts() {
        let trade = Trade {
            order: Order {
                order_creation: OrderCreation {
                    kind: OrderKind::Buy,
                    sell_amount: 10.into(),
                    buy_amount: 6.into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 5.into(),
            ..Default::default()
        };
        let sell_price = 3.into();
        let buy_price = 4.into();
        let execution = trade.executed_amounts(sell_price, buy_price).unwrap();

        assert_eq!(execution.sell_amount, 6.into()); // round down!
        assert_eq!(execution.buy_amount, 5.into());
    }

    #[test]
    fn trade_order_executed_amounts_overflow() {
        for kind in [OrderKind::Sell, OrderKind::Buy] {
            let order = Order {
                order_creation: OrderCreation {
                    kind,
                    ..Default::default()
                },
                ..Default::default()
            };

            // mul
            let trade = Trade {
                order: order.clone(),
                executed_amount: U256::MAX,
                ..Default::default()
            };
            let sell_price = U256::from(2);
            let buy_price = U256::one();
            assert!(trade.executed_amounts(sell_price, buy_price).is_none());

            // div
            let trade = Trade {
                order,
                executed_amount: U256::one(),
                ..Default::default()
            };
            let sell_price = U256::one();
            let buy_price = U256::zero();
            assert!(trade.executed_amounts(sell_price, buy_price).is_none());
        }
    }

    #[test]
    fn total_surplus() {
        let token0 = H160::from_low_u64_be(0);
        let token1 = H160::from_low_u64_be(1);

        let order0 = Order {
            order_creation: OrderCreation {
                sell_token: token0,
                buy_token: token1,
                sell_amount: 10.into(),
                buy_amount: 9.into(),
                kind: OrderKind::Sell,
                ..Default::default()
            },
            ..Default::default()
        };
        let order1 = Order {
            order_creation: OrderCreation {
                sell_token: token1,
                buy_token: token0,
                sell_amount: 10.into(),
                buy_amount: 9.into(),
                kind: OrderKind::Sell,
                ..Default::default()
            },
            ..Default::default()
        };

        let trade0 = Trade {
            order: order0.clone(),
            executed_amount: 10.into(),
            ..Default::default()
        };
        let trade1 = Trade {
            order: order1.clone(),
            executed_amount: 10.into(),
            ..Default::default()
        };

        // Case where external price vector doesn't influence ranking:

        let clearing_prices0 = maplit::hashmap! {token0 => 1.into(), token1 => 1.into()};
        let clearing_prices1 = maplit::hashmap! {token0 => 2.into(), token1 => 2.into()};

        let settlement0 = test_settlement(clearing_prices0, vec![trade0.clone(), trade1.clone()]);

        let settlement1 = test_settlement(clearing_prices1, vec![trade0, trade1]);

        let external_prices = maplit::hashmap! {token0 => r(1), token1 => r(1)};
        assert_eq!(
            settlement0.total_surplus(&external_prices),
            settlement1.total_surplus(&external_prices)
        );

        let external_prices = maplit::hashmap! {token0 => r(2), token1 => r(1)};
        assert_eq!(
            settlement0.total_surplus(&external_prices),
            settlement1.total_surplus(&external_prices)
        );

        // Case where external price vector influences ranking:

        let trade0 = Trade {
            order: order0.clone(),
            executed_amount: 10.into(),
            ..Default::default()
        };
        let trade1 = Trade {
            order: order1.clone(),
            executed_amount: 9.into(),
            ..Default::default()
        };

        let clearing_prices0 = maplit::hashmap! {token0 => 9.into(), token1 => 10.into()};

        // Settlement0 gets the following surpluses:
        // trade0: 81 - 81 = 0
        // trade1: 100 - 81 = 19
        let settlement0 = test_settlement(clearing_prices0, vec![trade0, trade1]);

        let trade0 = Trade {
            order: order0,
            executed_amount: 9.into(),
            ..Default::default()
        };
        let trade1 = Trade {
            order: order1,
            executed_amount: 10.into(),
            ..Default::default()
        };

        let clearing_prices1 = maplit::hashmap! {token0 => 10.into(), token1 => 9.into()};

        // Settlement1 gets the following surpluses:
        // trade0: 90 - 72.9 = 17.1
        // trade1: 100 - 100 = 0
        let settlement1 = test_settlement(clearing_prices1, vec![trade0, trade1]);

        // If the external prices of the two tokens is the same, then both settlements are symmetric.
        let external_prices = maplit::hashmap! {token0 => r(1), token1 => r(1)};
        assert_eq!(
            settlement0.total_surplus(&external_prices),
            settlement1.total_surplus(&external_prices)
        );

        // If the external price of the first token is higher, then the first settlement is preferred.
        let external_prices = maplit::hashmap! {token0 => r(2), token1 => r(1)};

        // Settlement0 gets the following normalized surpluses:
        // trade0: 0
        // trade1: 19 * 2 / 10 = 3.8

        // Settlement1 gets the following normalized surpluses:
        // trade0: 17.1 * 1 / 9 = 1.9
        // trade1: 0

        assert!(
            settlement0.total_surplus(&external_prices)
                > settlement1.total_surplus(&external_prices)
        );

        // If the external price of the second token is higher, then the second settlement is preferred.
        // (swaps above normalized surpluses of settlement0 and settlement1)
        let external_prices = maplit::hashmap! {token0 => r(1), token1 => r(2)};
        assert!(
            settlement0.total_surplus(&external_prices)
                < settlement1.total_surplus(&external_prices)
        );
    }

    #[test]
    fn test_computing_objective_value_with_zero_prices() {
        // Test if passing a clearing price of zero to the objective value function does
        // not panic.

        let token0 = H160::from_low_u64_be(0);
        let token1 = H160::from_low_u64_be(1);

        let order = Order {
            order_creation: OrderCreation {
                sell_token: token0,
                buy_token: token1,
                sell_amount: 10.into(),
                buy_amount: 9.into(),
                kind: OrderKind::Sell,
                ..Default::default()
            },
            ..Default::default()
        };

        let trade = Trade {
            order,
            executed_amount: 10.into(),
            ..Default::default()
        };

        let clearing_prices = maplit::hashmap! {token0 => 1.into(), token1 => 0.into()};

        let settlement = test_settlement(clearing_prices, vec![trade]);

        let external_prices = maplit::hashmap! {token0 => r(1), token1 => r(1)};
        settlement.total_surplus(&external_prices);
    }

    #[test]
    #[allow(clippy::just_underscores_and_digits)]
    fn test_buy_order_surplus() {
        // Two goods are worth the same (100 each). If we were willing to pay up to 60 to receive 50,
        // but ended paying the price (1) we have a surplus of 10 sell units, so a total surplus of 1000.

        assert_eq!(
            buy_order_surplus(&r(100), &r(100), &r(60), &r(50), &r(50)),
            Some(r(1000))
        );

        // If our trade got only half filled, we only get half the surplus
        assert_eq!(
            buy_order_surplus(&r(100), &r(100), &r(60), &r(50), &r(25)),
            Some(r(500))
        );

        // No surplus if trade is not at all filled
        assert_eq!(
            buy_order_surplus(&r(100), &r(100), &r(60), &r(50), &r(0)),
            Some(r(0))
        );

        // No surplus if trade is filled at limit
        assert_eq!(
            buy_order_surplus(&r(100), &r(100), &r(50), &r(50), &r(50)),
            Some(r(0))
        );

        // Arithmetic error when limit price not respected
        assert_eq!(
            buy_order_surplus(&r(100), &r(100), &r(40), &r(50), &r(50)),
            None
        );

        // Sell Token worth twice as much as buy token. If we were willing to sell at parity, we will
        // have a surplus of 50% of tokens, worth 200 each.
        assert_eq!(
            buy_order_surplus(&r(200), &r(100), &r(50), &r(50), &r(50)),
            Some(r(5000))
        );

        // Buy Token worth twice as much as sell token. If we were willing to sell at 3:1, we will
        // have a surplus of 20 sell tokens, worth 100 each.
        assert_eq!(
            buy_order_surplus(&r(100), &r(200), &r(60), &r(20), &r(20)),
            Some(r(2000))
        );
    }

    #[test]
    #[allow(clippy::just_underscores_and_digits)]
    fn test_sell_order_surplus() {
        // Two goods are worth the same (100 each). If we were willing to receive as little as 40,
        // but ended paying the price (1) we have a surplus of 10 bought units, so a total surplus of 1000.

        assert_eq!(
            sell_order_surplus(&r(100), &r(100), &r(50), &r(40), &r(50)),
            Some(r(1000))
        );

        // If our trade got only half filled, we only get half the surplus
        assert_eq!(
            sell_order_surplus(&r(100), &r(100), &r(50), &r(40), &r(25)),
            Some(r(500))
        );

        // No surplus if trade is not at all filled
        assert_eq!(
            sell_order_surplus(&r(100), &r(100), &r(50), &r(40), &r(0)),
            Some(r(0))
        );

        // No surplus if trade is filled at limit
        assert_eq!(
            sell_order_surplus(&r(100), &r(100), &r(50), &r(50), &r(50)),
            Some(r(0))
        );

        // Arithmetic error when limit price not respected
        assert_eq!(
            sell_order_surplus(&r(100), &r(100), &r(50), &r(60), &r(50)),
            None
        );

        // Sell token worth twice as much as buy token. If we were willing to buy at parity, we will
        // have a surplus of 100% of buy tokens, worth 100 each.
        assert_eq!(
            sell_order_surplus(&r(200), &r(100), &r(50), &r(50), &r(50)),
            Some(r(5000))
        );

        // Buy Token worth twice as much as sell token. If we were willing to buy at 3:1, we will
        // have a surplus of 10 sell tokens, worth 200 each.
        assert_eq!(
            buy_order_surplus(&r(100), &r(200), &r(60), &r(20), &r(20)),
            Some(r(2000))
        );
    }

    #[test]
    fn test_trade_fee() {
        let fully_filled_sell = Trade {
            order: Order {
                order_creation: OrderCreation {
                    sell_amount: 100.into(),
                    fee_amount: 5.into(),
                    kind: OrderKind::Sell,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 100.into(),
            ..Default::default()
        };
        assert_eq!(fully_filled_sell.executed_fee().unwrap(), 5.into());

        let partially_filled_sell = Trade {
            order: Order {
                order_creation: OrderCreation {
                    sell_amount: 100.into(),
                    fee_amount: 5.into(),
                    kind: OrderKind::Sell,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 50.into(),
            ..Default::default()
        };
        assert_eq!(partially_filled_sell.executed_fee().unwrap(), 2.into());

        let fully_filled_buy = Trade {
            order: Order {
                order_creation: OrderCreation {
                    buy_amount: 100.into(),
                    fee_amount: 5.into(),
                    kind: OrderKind::Buy,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 100.into(),
            ..Default::default()
        };
        assert_eq!(fully_filled_buy.executed_fee().unwrap(), 5.into());

        let partially_filled_buy = Trade {
            order: Order {
                order_creation: OrderCreation {
                    buy_amount: 100.into(),
                    fee_amount: 5.into(),
                    kind: OrderKind::Buy,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 50.into(),
            ..Default::default()
        };
        assert_eq!(partially_filled_buy.executed_fee().unwrap(), 2.into());
    }

    #[test]
    fn test_trade_fee_overflow() {
        let large_amounts = Trade {
            order: Order {
                order_creation: OrderCreation {
                    sell_amount: U256::max_value(),
                    fee_amount: U256::max_value(),
                    kind: OrderKind::Sell,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: U256::max_value(),
            ..Default::default()
        };
        assert_eq!(large_amounts.executed_fee(), None);

        let zero_amounts = Trade {
            order: Order {
                order_creation: OrderCreation {
                    sell_amount: U256::zero(),
                    fee_amount: U256::zero(),
                    kind: OrderKind::Sell,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: U256::zero(),
            ..Default::default()
        };
        assert_eq!(zero_amounts.executed_fee(), None);
    }

    #[test]
    fn total_fees_normalizes_individual_fees_into_eth() {
        let token0 = H160::from_low_u64_be(0);
        let token1 = H160::from_low_u64_be(1);

        let trade0 = Trade {
            order: Order {
                order_creation: OrderCreation {
                    sell_token: token0,
                    sell_amount: 10.into(),
                    fee_amount: 1.into(),
                    kind: OrderKind::Sell,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 10.into(),
            // Note that the scaled fee amount is different than the order's
            // signed fee amount. This happens for subsidized orders, and when
            // a fee objective scaling factor is configured.
            scaled_fee_amount: 5.into(),
            ..Default::default()
        };
        let trade1 = Trade {
            order: Order {
                order_creation: OrderCreation {
                    sell_token: token1,
                    sell_amount: 10.into(),
                    fee_amount: 2.into(),
                    kind: OrderKind::Sell,
                    ..Default::default()
                },
                ..Default::default()
            },
            executed_amount: 10.into(),
            scaled_fee_amount: 2.into(),
            ..Default::default()
        };

        let clearing_prices = maplit::hashmap! {token0 => 5.into(), token1 => 10.into()};
        let external_prices = maplit::hashmap! {token0 => BigRational::from_integer(5.into()), token1 => BigRational::from_integer(10.into())};

        // Fee in sell tokens
        assert_eq!(trade0.executed_fee().unwrap(), 1.into());
        assert_eq!(trade0.executed_scaled_unsubsidized_fee().unwrap(), 5.into());
        assert_eq!(trade1.executed_fee().unwrap(), 2.into());
        assert_eq!(trade1.executed_scaled_unsubsidized_fee().unwrap(), 2.into());

        // Fee in wei of ETH
        let settlement = test_settlement(clearing_prices, vec![trade0, trade1]);
        assert_eq!(
            settlement.total_scaled_unsubsidized_fees(&external_prices),
            BigRational::from_integer(45.into())
        );
    }

    #[test]
    fn total_surplus_excludes_liquidity_orders() {
        let token0 = H160::from_low_u64_be(0);
        let token1 = H160::from_low_u64_be(1);

        let order0 = Order {
            order_creation: OrderCreation {
                sell_token: token0,
                buy_token: token1,
                sell_amount: 10.into(),
                buy_amount: 7.into(),
                kind: OrderKind::Sell,
                ..Default::default()
            },
            ..Default::default()
        };
        let order1 = Order {
            order_creation: OrderCreation {
                sell_token: token1,
                buy_token: token0,
                sell_amount: 10.into(),
                buy_amount: 6.into(),
                kind: OrderKind::Sell,
                ..Default::default()
            },
            ..Default::default()
        };

        let trade0 = Trade {
            order: order0,
            executed_amount: 10.into(),
            is_liquidity_order: false,
            ..Default::default()
        };
        let trade1 = Trade {
            order: order1,
            executed_amount: 9.into(),
            is_liquidity_order: false,
            ..Default::default()
        };

        let clearing_prices = maplit::hashmap! {token0 => 9.into(), token1 => 10.into()};
        let external_prices = maplit::hashmap! {token0 => r(1), token1 => r(1)};

        let compute_total_surplus = |trade0_is_pmm: bool, trade1_is_pmm: bool| {
            let settlement = test_settlement(
                clearing_prices.clone(),
                vec![
                    Trade {
                        is_liquidity_order: trade0_is_pmm,
                        ..trade0.clone()
                    },
                    Trade {
                        is_liquidity_order: trade1_is_pmm,
                        ..trade1.clone()
                    },
                ],
            );
            settlement.total_surplus(&external_prices)
        };

        let total_surplus_without_pmms = compute_total_surplus(false, false);
        let surplus_order1 = compute_total_surplus(false, true);
        let surplus_order0 = compute_total_surplus(true, false);
        let both_pmm = compute_total_surplus(true, true);
        assert_eq!(total_surplus_without_pmms, surplus_order0 + surplus_order1);
        assert!(both_pmm.is_zero());
    }

    #[test]
    fn fees_excluded_for_pmm_orders() {
        let token0 = H160([0; 20]);
        let token1 = H160([1; 20]);
        let settlement = test_settlement(
            hashmap! { token0 => 1.into(), token1 => 1.into() },
            vec![
                Trade {
                    order: Order {
                        order_creation: OrderCreation {
                            sell_token: token0,
                            buy_token: token1,
                            sell_amount: 1.into(),
                            kind: OrderKind::Sell,
                            // Note that this fee amount is NOT used!
                            fee_amount: 6.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    executed_amount: 1.into(),
                    // This is what matters for the objective value
                    scaled_fee_amount: 42.into(),
                    is_liquidity_order: false,
                    ..Default::default()
                },
                Trade {
                    order: Order {
                        order_creation: OrderCreation {
                            sell_token: token1,
                            buy_token: token0,
                            buy_amount: 1.into(),
                            kind: OrderKind::Buy,
                            fee_amount: 28.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    executed_amount: 1.into(),
                    // Doesn't count because it is a "liquidity order"
                    scaled_fee_amount: 1337.into(),
                    is_liquidity_order: true,
                    ..Default::default()
                },
            ],
        );

        assert_eq!(
            settlement.total_scaled_unsubsidized_fees(&hashmap! { token0 => r(1) }),
            r(42),
        );
    }

    #[test]
    fn prefers_amm_with_better_price_over_pmm() {
        let amm = test_settlement(
            hashmap! {
                addr!("4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b") => 99760667014_u128.into(),
                addr!("dac17f958d2ee523a2206206994597c13d831ec7") => 3813250751402140530019_u128.into(),
            },
            vec![Trade {
                order: Order {
                    order_creation: OrderCreation {
                        sell_token: addr!("dac17f958d2ee523a2206206994597c13d831ec7"),
                        buy_token: addr!("4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b"),
                        sell_amount: 99760667014_u128.into(),
                        buy_amount: 3805639472457226077863_u128.into(),
                        fee_amount: 239332986_u128.into(),
                        kind: OrderKind::Sell,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                executed_amount: 99760667014_u128.into(),
                scaled_fee_amount: 239332986_u128.into(),
                is_liquidity_order: false,
                ..Default::default()
            }],
        );

        let pmm = test_settlement(
            hashmap! {
                addr!("4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b") => 6174583113007029_u128.into(),
                addr!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48") => 235665799111775530988005794_u128.into(),
                addr!("dac17f958d2ee523a2206206994597c13d831ec7") => 235593507027683452564881428_u128.into(),
            },
            vec![
                Trade {
                    order: Order {
                        order_creation: OrderCreation {
                            sell_token: addr!("dac17f958d2ee523a2206206994597c13d831ec7"),
                            buy_token: addr!("4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b"),
                            sell_amount: 99760667014_u128.into(),
                            buy_amount: 3805639472457226077863_u128.into(),
                            fee_amount: 239332986_u128.into(),
                            kind: OrderKind::Sell,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    executed_amount: 99760667014_u128.into(),
                    scaled_fee_amount: 239332986_u128.into(),
                    is_liquidity_order: false,
                    ..Default::default()
                },
                Trade {
                    order: Order {
                        order_creation: OrderCreation {
                            sell_token: addr!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
                            buy_token: addr!("dac17f958d2ee523a2206206994597c13d831ec7"),
                            sell_amount: 99730064753_u128.into(),
                            buy_amount: 99760667014_u128.into(),
                            fee_amount: 10650127_u128.into(),
                            kind: OrderKind::Buy,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    executed_amount: 99760667014_u128.into(),
                    scaled_fee_amount: 77577144_u128.into(),
                    is_liquidity_order: true,
                    ..Default::default()
                },
            ],
        );

        let external_prices = hashmap! {
            addr!("4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b") =>
                BigRational::new(250000000000000000_u128.into(), 40551883611992959283_u128.into()),
            addr!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48") =>
                BigRational::new(40000000000000000_u128.into(), 168777939_u128.into()),
            addr!("dac17f958d2ee523a2206206994597c13d831ec7") =>
                BigRational::new(250000000000000000_u128.into(), 1055980021_u128.into()),
        };
        let gas_price = 105386573044;
        let objective_value = |settlement: &Settlement, gas: u128| {
            settlement.total_surplus(&external_prices)
                + settlement.total_scaled_unsubsidized_fees(&external_prices)
                - r(gas * gas_price)
        };

        // Prefer the AMM that uses more gas because it offers a better price
        // to the user!
        assert!(objective_value(&amm, 657196) > objective_value(&pmm, 405053));
    }
}
