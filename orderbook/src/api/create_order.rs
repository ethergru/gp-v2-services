use crate::{
    api::{extract_payload, IntoWarpReply},
    orderbook::{AddOrderResult, Orderbook},
};
use anyhow::Result;
use model::order::OrderCreationPayload;
use std::{convert::Infallible, sync::Arc};
use warp::reply::with_status;
use warp::{hyper::StatusCode, Filter, Rejection, Reply};

pub fn create_order_request(
) -> impl Filter<Extract = (OrderCreationPayload,), Error = Rejection> + Clone {
    warp::path!("orders")
        .and(warp::post())
        .and(extract_payload())
}

pub fn create_order_response(result: Result<AddOrderResult>) -> impl Reply {
    match result {
        Ok(AddOrderResult::Added(uid)) => with_status(warp::reply::json(&uid), StatusCode::CREATED),
        Ok(AddOrderResult::OrderValidation(err)) => err.into_warp_reply(),
        Ok(AddOrderResult::UnsupportedSignature) => with_status(
            super::error("UnsupportedSignature", "signing scheme is not supported"),
            StatusCode::BAD_REQUEST,
        ),
        Ok(AddOrderResult::DuplicatedOrder) => with_status(
            super::error("DuplicatedOrder", "order already exists"),
            StatusCode::BAD_REQUEST,
        ),
        Err(err) => with_status(
            super::internal_error(err.context("create_order")),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

pub fn create_order(
    orderbook: Arc<Orderbook>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    create_order_request().and_then(move |order_payload: OrderCreationPayload| {
        let orderbook = orderbook.clone();
        async move {
            let order_payload_clone = order_payload.clone();
            let result = orderbook.add_order(order_payload).await;
            // TODO - This is one place where the error log is more rich than the
            //  generic error inside internal_error (i.e. doesn't include order_payload).
            //  Perhaps this should be a warning and the real alert comes from the internal error.
            //  Otherwise we should just resort to using this error logging style everywhere.
            if let Err(err) = &result {
                tracing::error!(?err, ?order_payload_clone, "add_order error");
            }
            Result::<_, Infallible>::Ok(create_order_response(result))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::response_body;
    use model::order::{OrderCreationPayload, OrderUid};
    use serde_json::json;
    use warp::test::request;

    #[tokio::test]
    async fn create_order_request_ok() {
        let filter = create_order_request();
        let order_payload = OrderCreationPayload::default();
        let request = request()
            .path("/orders")
            .method("POST")
            .header("content-type", "application/json")
            .json(&order_payload);
        let result = request.filter(&filter).await.unwrap();
        assert_eq!(result, order_payload);
    }

    #[tokio::test]
    async fn create_order_response_created() {
        let uid = OrderUid([1u8; 56]);
        let response = create_order_response(Ok(AddOrderResult::Added(uid))).into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response_body(response).await;
        let body: serde_json::Value = serde_json::from_slice(body.as_slice()).unwrap();
        let expected= json!(
            "0x0101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101"
        );
        assert_eq!(body, expected);
    }

    #[tokio::test]
    async fn create_order_response_duplicate() {
        let response = create_order_response(Ok(AddOrderResult::DuplicatedOrder)).into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_body(response).await;
        let body: serde_json::Value = serde_json::from_slice(body.as_slice()).unwrap();
        let expected_error =
            json!({"errorType": "DuplicatedOrder", "description": "order already exists"});
        assert_eq!(body, expected_error);
    }
}
