use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::{github::GitHubError, models::ApiErrorBody};

pub struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) body: ApiErrorBody,
    /// Seconds the client should wait before retrying. Set for errors that
    /// are transient by nature (upstream outage, rate limit) so well-behaved
    /// clients back off instead of hammering a struggling dependency.
    pub(crate) retry_after: Option<u64>,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiErrorBody {
                code: code.to_string(),
                message: message.into(),
                default_branch: None,
            },
            retry_after: None,
        }
    }

    /// Attaches the repository's default branch to the body so clients that
    /// localize by `code` can still name it.
    fn with_default_branch(mut self, branch: Option<String>) -> Self {
        self.body.default_branch = branch;
        self
    }

    fn with_retry_after(mut self, seconds: u64) -> Self {
        self.retry_after = Some(seconds);
        self
    }

    pub(crate) fn internal(error: anyhow::Error) -> Self {
        tracing::error!(%error, "internal error");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal server error",
        )
    }

    pub(crate) fn not_found(code: &str, message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    /// The service's own rate limit, as opposed to a relayed upstream one.
    /// `retry_after` is the exact wait the limiter computed.
    pub(crate) fn rate_limited(retry_after: u64) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many requests; please retry later",
        )
        .with_retry_after(retry_after)
    }

    pub(crate) fn body(&self) -> &ApiErrorBody {
        &self.body
    }
}

impl From<GitHubError> for ApiError {
    fn from(error: GitHubError) -> Self {
        match error {
            GitHubError::InvalidUrl => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_url", error.to_string())
            }
            GitHubError::NotFound => {
                Self::new(StatusCode::NOT_FOUND, "not_found", error.to_string())
            }
            GitHubError::PrivateRepo => {
                Self::new(StatusCode::FORBIDDEN, "private_repo", error.to_string())
            }
            // The code stays `ref_not_found` — clients switch on it — but the
            // message names the branch that does exist. Without it a
            // `main`-vs-`master` mismatch looks like a broken repository rather
            // than a wrong ref, and the caller has no way to find the right one.
            GitHubError::RefNotFound { ref default_branch } => {
                let message = match default_branch {
                    Some(branch) => format!(
                        "requested ref was not found; this repository's default branch is \"{branch}\""
                    ),
                    None => error.to_string(),
                };
                Self::new(StatusCode::NOT_FOUND, "ref_not_found", message)
                    .with_default_branch(default_branch.clone())
            }
            GitHubError::EmptyRepository => {
                Self::new(StatusCode::NOT_FOUND, "empty_repository", error.to_string())
            }
            GitHubError::RateLimited => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                error.to_string(),
            )
            // The anonymous quota resets hourly, the token quota every hour
            // too; a minute is enough for client backoff without pretending
            // we know GitHub's exact reset time.
            .with_retry_after(60),
            GitHubError::UpstreamUnavailable => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "github_unavailable",
                error.to_string(),
            )
            // GitHub incidents typically resolve in minutes; retries sooner
            // only add load to a struggling upstream.
            .with_retry_after(120),
            GitHubError::TooLarge => Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "too_large",
                error.to_string(),
            ),
            GitHubError::Request(_) => Self::new(
                StatusCode::BAD_GATEWAY,
                "github_request_failed",
                error.to_string(),
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(self.body)).into_response();
        if let Some(seconds) = self.retry_after {
            if let Ok(value) = axum::http::HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upstream_unavailable_maps_to_503_with_retry_after() {
        let response = ApiError::from(GitHubError::UpstreamUnavailable).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok()),
            Some("120")
        );
        let body: ApiErrorBody = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 4096).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body.code, "github_unavailable");
    }

    async fn error_body(error: GitHubError) -> ApiErrorBody {
        let response = ApiError::from(error).into_response();
        serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 4096).await.unwrap())
            .unwrap()
    }

    /// The whole point of the `main`-vs-`master` fix on the reporting side: the
    /// code stays stable for clients that switch on it, the message says which
    /// branch exists, and the branch also travels as data for clients that
    /// replace the message with a localized one.
    #[tokio::test]
    async fn ref_not_found_names_the_repositorys_default_branch() {
        let body = error_body(GitHubError::RefNotFound {
            default_branch: Some("master".to_string()),
        })
        .await;
        assert_eq!(body.code, "ref_not_found");
        assert_eq!(body.default_branch.as_deref(), Some("master"));
        assert!(body.message.contains("master"), "message was {}", body.message);
    }

    /// Nothing to suggest, nothing claimed — and no `defaultBranch` key at all,
    /// so a client cannot mistake absence for an empty branch name.
    #[tokio::test]
    async fn ref_not_found_without_a_known_default_branch_stays_generic() {
        let error = ApiError::from(GitHubError::RefNotFound {
            default_branch: None,
        });
        assert_eq!(
            serde_json::to_value(error.body()).unwrap(),
            serde_json::json!({
                "code": "ref_not_found",
                "message": "requested ref was not found",
            })
        );
    }

    /// An empty repository is not a missing ref: the caller supplied none.
    #[tokio::test]
    async fn empty_repository_gets_its_own_code() {
        let response = ApiError::from(GitHubError::EmptyRepository).into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: ApiErrorBody = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 4096).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body.code, "empty_repository");
    }

    #[tokio::test]
    async fn rate_limited_keeps_429_and_gains_retry_after() {
        let response = ApiError::from(GitHubError::RateLimited).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok()),
            Some("60")
        );
    }
}
