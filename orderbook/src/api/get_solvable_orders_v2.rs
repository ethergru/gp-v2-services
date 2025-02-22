use crate::{api::convert_json_response, orderbook::Orderbook};
use anyhow::Result;
use std::{convert::Infallible, sync::Arc};
use warp::{Filter, Rejection, Reply};

fn get_solvable_orders_request() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("solvable_orders").and(warp::get())
}

pub fn get_solvable_orders(
    orderbook: Arc<Orderbook>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    get_solvable_orders_request().and_then(move || {
        let orderbook = orderbook.clone();
        async move {
            let result = orderbook.get_solvable_orders().await;
            Result::<_, Infallible>::Ok(convert_json_response(result.map(|orders| {
                model::SolvableOrders {
                    orders: orders.orders,
                    latest_settlement_block: orders.latest_settlement_block,
                }
            })))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::response_body;
    use model::SolvableOrders;
    use warp::hyper::StatusCode;

    #[tokio::test]
    async fn serialize_response() {
        let solvable_orders = SolvableOrders {
            orders: vec![],
            latest_settlement_block: 1,
        };
        let response =
            convert_json_response::<_, anyhow::Error>(Ok(solvable_orders)).into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        let response: model::SolvableOrders = serde_json::from_slice(body.as_slice()).unwrap();
        assert_eq!(response.latest_settlement_block, 1);
    }
}
