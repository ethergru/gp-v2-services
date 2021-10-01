use std::time::Duration;

use anyhow::{Context, Result};
use contracts::GPv2Settlement;
use ethcontract::{dyns::DynMethodBuilder, errors::ExecutionError, Account};
use futures::FutureExt;
use primitive_types::{H160, H256, U256};
use shared::Web3;
use web3::types::TransactionId;

use crate::encoding::EncodedSettlement;

/// From a list of potential hashes find one that was mined.
pub async fn find_mined_transaction(web3: &Web3, hashes: &[H256]) -> Option<H256> {
    // It would be nice to use the nonce and account address to find the transaction hash but there
    // is no way to do this in ethrpc api so we have to check the candidates one by one.
    let web3 = web3::Web3::new(web3::transports::Batch::new(web3.transport()));
    let futures = hashes
        .iter()
        .map(|&hash| web3.eth().transaction(TransactionId::Hash(hash)))
        .collect::<Vec<_>>();
    if let Err(err) = web3.transport().submit_batch().await {
        tracing::error!("mined transaction batch failed: {:?}", err);
        return None;
    }
    for future in futures {
        match future.now_or_never().unwrap() {
            Err(err) => {
                tracing::error!("mined transaction individual failed: {:?}", err);
            }
            Ok(Some(transaction)) if transaction.block_hash.is_some() => {
                return Some(transaction.hash)
            }
            Ok(_) => (),
        }
    }
    None
}

pub async fn nonce(web3: &Web3, address: H160) -> Result<U256> {
    web3.eth()
        .transaction_count(address, None)
        .await
        .context("transaction_count")
}

/// Keep polling the account's nonce until it is different from initial_nonce returning the new
/// nonce.
pub async fn wait_for_nonce_to_change(web3: &Web3, address: H160, initial_nonce: U256) -> U256 {
    const POLL_INTERVAL: Duration = Duration::from_secs(1);
    loop {
        match nonce(web3, address).await {
            Ok(nonce) if nonce != initial_nonce => return nonce,
            Ok(_) => (),
            Err(err) => tracing::error!("web3 error while getting nonce: {:?}", err),
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Failure indicating the transaction reverted for some reason
pub fn is_transaction_failure(error: &ExecutionError) -> bool {
    matches!(error, ExecutionError::Failure(_))
        || matches!(error, ExecutionError::Revert(_))
        || matches!(error, ExecutionError::InvalidOpcode)
}

pub fn settle_method_builder(
    contract: &GPv2Settlement,
    settlement: EncodedSettlement,
    from: Account,
) -> DynMethodBuilder<()> {
    contract
        .settle(
            settlement.tokens,
            settlement.clearing_prices,
            settlement.trades,
            settlement.interactions,
        )
        .from(from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_transaction_failure() {
        // Positives
        assert!(is_transaction_failure(&ExecutionError::Failure(
            Default::default()
        )),);
        assert!(is_transaction_failure(&ExecutionError::Revert(None)));
        assert!(is_transaction_failure(&ExecutionError::InvalidOpcode));

        // Sample negative
        assert!(!is_transaction_failure(&ExecutionError::ConfirmTimeout(
            Box::new(ethcontract::transaction::TransactionResult::Hash(
                H256::default()
            ))
        )));
    }
}
