use crate::{api::convert_json_response, orderbook::Orderbook};
use anyhow::Result;
use primitive_types::H160;
use serde::Deserialize;
use std::{convert::Infallible, sync::Arc};
use warp::{hyper::StatusCode, reply::with_status, Filter, Rejection, Reply};

#[derive(Clone, Copy, Debug, Deserialize)]
struct Query {
    offset: Option<u64>,
    limit: Option<u64>,
}

fn request() -> impl Filter<Extract = (H160, Query), Error = Rejection> + Clone {
    warp::path!("account" / H160 / "orders")
        .and(warp::get())
        .and(warp::query::<Query>())
}

pub fn get_user_orders(
    orderbook: Arc<Orderbook>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    request().and_then(move |owner: H160, query: Query| {
        let orderbook = orderbook.clone();
        async move {
            const DEFAULT_OFFSET: u64 = 0;
            const DEFAULT_LIMIT: u64 = 10;
            const MIN_LIMIT: u64 = 1;
            const MAX_LIMIT: u64 = 1000;
            let offset = query.offset.unwrap_or(DEFAULT_OFFSET);
            let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
            if !(MIN_LIMIT..=MAX_LIMIT).contains(&limit) {
                return Ok(with_status(
                    super::error(
                        "LIMIT_OUT_OF_BOUNDS",
                        &format!("The pagination limit is [{},{}].", MIN_LIMIT, MAX_LIMIT),
                    ),
                    StatusCode::BAD_REQUEST,
                ));
            }
            let result = orderbook.get_user_orders(&owner, offset, limit).await;
            Result::<_, Infallible>::Ok(convert_json_response(result))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::addr;

    #[tokio::test]
    async fn request_() {
        let path = "/account/0x0000000000000000000000000000000000000001/orders";
        let result = warp::test::request()
            .path(path)
            .method("GET")
            .filter(&request())
            .await
            .unwrap();
        assert_eq!(result.0, addr!("0000000000000000000000000000000000000001"));
        assert_eq!(result.1.offset, None);
        assert_eq!(result.1.limit, None);

        let path = "/account/0x0000000000000000000000000000000000000001/orders?offset=1&limit=2";
        let result = warp::test::request()
            .path(path)
            .method("GET")
            .filter(&request())
            .await
            .unwrap();
        assert_eq!(result.1.offset, Some(1));
        assert_eq!(result.1.limit, Some(2));
    }
}
