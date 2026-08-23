use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::TryStreamExt;
use moka::{future::Cache, policy::EvictionPolicy};
use reqwest::{header, Client, StatusCode};
use serde::Deserialize;
use thiserror::Error;
use tokio_util::io::{StreamReader, SyncIoBridge};
use url::Url;

use crate::models::{RepoRef, RepositoryProvider};

/// How long a resolved `ref -> commit sha` mapping stays trustworthy.
///
/// Branch refs move. Without an expiry the entry for `main` is pinned to the
/// first commit we ever saw, so every later request replays a stale report and
/// only `force_refresh` can break the loop. Tags and commit shas are immutable,
/// but they are keyed the same way and re-resolving them is one cheap API call,
/// so a single short TTL covers both. 60s keeps the burst-protection value of
/// the cache (a page that fires several requests still resolves once) while
/// bounding staleness to something a user would not notice.
const REF_CACHE_TTL: Duration = Duration::from_secs(60);
const REF_CACHE_CAPACITY: u64 = 10_000;

const GRAPHQL_ENDPOINT: &str = "https://api.github.com/graphql";

/// One request for everything REST needs two for: visibility, canonical URL, the
/// default branch, and the commit the requested ref points at.
///
/// `object(expression:)` resolves the same grammar as
/// `GET /repos/{o}/{r}/commits/{ref}` — branch, tag or raw sha — and GitHub
/// peels annotated tags to their commit before returning, so `torvalds/linux`
/// at `v6.6` yields the same sha through both paths. It is skipped entirely
/// when no ref was requested, because the default branch's target is already in
/// hand.
const REF_QUERY: &str = "\
query($owner:String!,$name:String!,$expression:String!,$hasRef:Boolean!){\
repository(owner:$owner,name:$name){\
isPrivate url stargazerCount \
defaultBranchRef{name target{oid}} \
object(expression:$expression)@include(if:$hasRef){__typename oid}}}";

#[derive(Debug, Error)]
pub enum GitHubError {
    #[error("only public github.com and gitlab.com repository URLs are supported")]
    InvalidUrl,
    #[error("repository was not found or is not public")]
    NotFound,
    #[error("private repositories are not supported")]
    PrivateRepo,
    #[error("GitHub API rate limit was reached")]
    RateLimited,
    #[error("repository archive is too large")]
    TooLarge,
    /// Carries the repository's real default branch when the resolver knew it.
    /// Both resolution paths have it in hand at exactly the moment they decide
    /// the requested ref is unresolvable — GraphQL because `REF_QUERY` always
    /// selects `defaultBranchRef`, REST because the repo call precedes the
    /// commit call — and dropping it is what makes a `main`-vs-`master`
    /// mismatch undiagnosable from the client side.
    #[error("requested ref was not found")]
    RefNotFound { default_branch: Option<String> },
    /// Distinct from [`GitHubError::RefNotFound`]: the caller asked for no ref
    /// at all and there is no default branch to fall back to. Reporting that as
    /// "requested ref was not found" tells a user their ref is missing when
    /// they never supplied one.
    #[error("repository is empty and has no default branch")]
    EmptyRepository,
    /// GitHub/GitLab itself is failing (5xx or timeouts that survive retries).
    /// Distinct from [`GitHubError::Request`] so clients can tell an upstream
    /// outage apart from a bug in this service.
    #[error("the repository host is currently unavailable; please try again later")]
    UpstreamUnavailable,
    #[error("GitHub request failed")]
    Request(#[from] reqwest::Error),
}

/// Backoff schedule for upstream GETs. Short by design: this rides out
/// transient 5xx blips without stretching an analysis minutes past the
/// client's patience. Exhausting the schedule turns 5xx/timeout into
/// [`GitHubError::UpstreamUnavailable`] instead of a misleading not-found.
const UPSTREAM_RETRY_DELAYS: [Duration; 2] = [Duration::from_millis(500), Duration::from_millis(1500)];

/// Longest we will park a request waiting for an upstream rate limit to
/// lift. Beyond this the response is handed back and the caller reports a
/// rate limit rather than holding a connection open for minutes.
const RATE_LIMIT_SLEEP_CAP: Duration = Duration::from_secs(60);

/// How many header-directed rate-limit waits one request may take before it
/// gives up and surfaces the rate limit to the caller.
const MAX_RATE_LIMIT_SLEEPS: usize = 2;

#[derive(Clone)]
pub struct GitHubClient {
    client: Client,
    /// Long-timeout twin of `client` for archive downloads, where a multi-
    /// hundred-megabyte body legitimately outruns a JSON call's budget.
    archive_client: Client,
    ref_cache: Cache<(String, Option<String>), RepoRef>,
    stars_cache: Cache<(RepositoryProvider, String, String), Option<u64>>,
    /// GitHub's GraphQL API rejects unauthenticated requests outright, and
    /// `GITHUB_TOKEN` is optional in this deployment, so the fast path is only
    /// attempted when there is a token to attempt it with.
    has_token: bool,
    /// Every 429 seen from an upstream, across retries. Cheap observability
    /// for `/internal/stats`; shared through `Arc` so clones of this client
    /// (one per coordinator) report one number.
    rate_limited_429: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
struct RepoResponse {
    /// Defensive only. GitHub's `full-repository` schema makes this required and
    /// it is present even for a repository with no commits — an empty repo
    /// reports the branch the setting names, which is why the commits endpoint's
    /// 409 is what identifies that state and not a missing field here. Kept so a
    /// schema change cannot fail the whole deserialization; the empty string is
    /// then filtered out at the one call site.
    #[serde(default)]
    default_branch: String,
    html_url: String,
    private: bool,
    #[serde(default)]
    stargazers_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CommitResponse {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct GitLabProjectResponse {
    id: u64,
    path_with_namespace: String,
    /// Missing whenever the caller cannot read the code: GitLab exposes
    /// `default_branch` (alongside the clone URLs) only to callers holding
    /// `read_code`, which a *public* project denies when its repository feature
    /// is member-only or disabled. A project with no commits is a different
    /// state and says so with `empty_repo`, so this being absent is not
    /// evidence of emptiness — see the one call site, which answers
    /// "not readable" rather than "empty". `Option` also keeps a schema change
    /// on a field this resolver barely needs from failing the whole response.
    #[serde(default)]
    default_branch: Option<String>,
    web_url: String,
    visibility: String,
    #[serde(default)]
    star_count: Option<u64>,
    /// GitLab states emptiness outright, where the GitHub path has to read it
    /// off a 409 from the commits endpoint. Defaulting to `false` fails towards
    /// "has commits", which is the safe direction: the worst outcome is a ref
    /// lookup that fails on its own terms. If GitLab gates this on `read_code`
    /// as well, an empty *and* unreadable project falls through to the
    /// not-readable answer below — indistinguishable from here, and no more
    /// wrong than the alternative.
    #[serde(default)]
    empty_repo: bool,
}

#[derive(Debug, Deserialize)]
struct GitLabCommitResponse {
    id: String,
}

#[derive(Debug, Deserialize)]
struct GraphQlResponse {
    #[serde(default)]
    data: Option<GraphQlData>,
    #[serde(default)]
    errors: Vec<GraphQlError>,
}

#[derive(Debug, Deserialize)]
struct GraphQlData {
    #[serde(default)]
    repository: Option<GraphQlRepository>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphQlRepository {
    is_private: bool,
    url: String,
    #[serde(default)]
    stargazer_count: Option<u64>,
    #[serde(default)]
    default_branch_ref: Option<GraphQlRef>,
    /// Absent when `hasRef` was false, null when the expression resolved to
    /// nothing.
    #[serde(default)]
    object: Option<GraphQlObject>,
}

#[derive(Debug, Deserialize)]
struct GraphQlRef {
    name: String,
    #[serde(default)]
    target: Option<GraphQlOid>,
}

#[derive(Debug, Deserialize)]
struct GraphQlOid {
    oid: String,
}

#[derive(Debug, Deserialize)]
struct GraphQlObject {
    #[serde(rename = "__typename")]
    typename: String,
    oid: String,
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    #[serde(rename = "type", default)]
    error_type: Option<String>,
}

/// What a GraphQL response told us.
///
/// The third arm is the important one: GraphQL is an optimisation, not a
/// replacement. Anything this code does not positively recognise — an
/// unexpected object type, an error class it has no mapping for, a 500 — defers
/// to the REST path rather than guessing, so a change on GitHub's side degrades
/// to the old latency instead of to a wrong answer.
#[derive(Debug, PartialEq, Eq)]
enum GraphQlOutcome {
    Resolved {
        html_url: String,
        ref_name: String,
        stars: Option<u64>,
        commit_sha: String,
    },
    Failed(GraphQlFailure),
    Unusable,
}

/// The subset of [`GitHubError`] a GraphQL response can decide on its own.
/// Separate because `GitHubError` is not `PartialEq` (it wraps `reqwest::Error`)
/// and these outcomes need to be asserted on directly.
///
/// Not `Copy`: `RefNotFound` carries the default branch so the API error can
/// name it.
#[derive(Debug, PartialEq, Eq, Clone)]
enum GraphQlFailure {
    NotFound,
    PrivateRepo,
    RateLimited,
    RefNotFound { default_branch: Option<String> },
    EmptyRepository,
}

impl From<GraphQlFailure> for GitHubError {
    fn from(failure: GraphQlFailure) -> Self {
        match failure {
            GraphQlFailure::NotFound => GitHubError::NotFound,
            GraphQlFailure::PrivateRepo => GitHubError::PrivateRepo,
            GraphQlFailure::RateLimited => GitHubError::RateLimited,
            GraphQlFailure::RefNotFound { default_branch } => {
                GitHubError::RefNotFound { default_branch }
            }
            GraphQlFailure::EmptyRepository => GitHubError::EmptyRepository,
        }
    }
}

/// Turns a GraphQL body into an outcome. Pure, so the mapping is testable
/// without a network.
fn interpret_graphql(response: GraphQlResponse, requested_ref: Option<&str>) -> GraphQlOutcome {
    let Some(repository) = response.data.and_then(|data| data.repository) else {
        return classify_graphql_errors(&response.errors);
    };

    if repository.is_private {
        return GraphQlOutcome::Failed(GraphQlFailure::PrivateRepo);
    }

    let default_branch = repository
        .default_branch_ref
        .as_ref()
        .map(|branch| branch.name.clone())
        .filter(|name| !name.is_empty());

    match requested_ref {
        Some(ref_name) => {
            let Some(object) = repository.object else {
                // No default branch and nothing resolved means the repository
                // has no commits, so the ref was never the problem. REST reaches
                // this from a 409 on the commits endpoint; without this arm the
                // two paths returned different error bodies for the same
                // request, which the parity invariant forbids.
                //
                // A repository whose HEAD points at a deleted branch while other
                // branches still have commits would be misreported here, but
                // GitHub does not produce that state on its own and the previous
                // answer for it — a bare `ref_not_found` — was no better.
                if default_branch.is_none() {
                    return GraphQlOutcome::Failed(GraphQlFailure::EmptyRepository);
                }
                return GraphQlOutcome::Failed(GraphQlFailure::RefNotFound { default_branch });
            };
            // GitHub resolves annotated tags to their commit, so anything else
            // is a shape this code has not seen; let REST answer it.
            if object.typename != "Commit" {
                return GraphQlOutcome::Unusable;
            }
            GraphQlOutcome::Resolved {
                html_url: repository.url,
                ref_name: ref_name.to_string(),
                stars: repository.stargazer_count,
                commit_sha: object.oid,
            }
        }
        None => {
            // An empty repository has no default branch. REST reaches the same
            // conclusion from the commits endpoint — a 409 for the repository,
            // or a 404 on the branch `/repos` named — and both now report it as
            // `EmptyRepository` rather than blaming a ref the caller never
            // supplied.
            let Some(target) = repository
                .default_branch_ref
                .as_ref()
                .and_then(|branch| branch.target.as_ref())
            else {
                return GraphQlOutcome::Failed(GraphQlFailure::EmptyRepository);
            };
            GraphQlOutcome::Resolved {
                html_url: repository.url.clone(),
                ref_name: default_branch.unwrap_or_default(),
                stars: repository.stargazer_count,
                commit_sha: target.oid.clone(),
            }
        }
    }
}

fn classify_graphql_errors(errors: &[GraphQlError]) -> GraphQlOutcome {
    let has = |wanted: &str| {
        errors
            .iter()
            .any(|error| error.error_type.as_deref() == Some(wanted))
    };
    if has("RATE_LIMITED") {
        GraphQlOutcome::Failed(GraphQlFailure::RateLimited)
    } else if has("NOT_FOUND") {
        // Also what a private repository the token cannot see looks like —
        // exactly as REST reports it.
        GraphQlOutcome::Failed(GraphQlFailure::NotFound)
    } else {
        GraphQlOutcome::Unusable
    }
}

/// How long a 429 (or rate-limiting 403) response says to wait, taken from
/// `retry-after` (seconds) or, failing that, `x-ratelimit-reset` (epoch
/// seconds). `None` when the response carries neither, leaving the fixed
/// backoff schedule in charge.
fn rate_limit_wait(response: &reqwest::Response) -> Option<Duration> {
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    if let Some(seconds) = retry_after {
        return Some(Duration::from_secs(seconds));
    }
    let reset_epoch = response
        .headers()
        .get("x-ratelimit-reset")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<i64>().ok())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    Some(Duration::from_secs((reset_epoch - now).max(0) as u64))
}

#[derive(Debug, Clone)]
struct RepoTarget {
    provider: RepositoryProvider,
    owner: String,
    repo: String,
    path: String,
    /// Ref candidates read out of the pasted URL's trailing path
    /// (`/tree/<ref>`, `/blob/<ref>/<file>`, `/commit/<sha>`), best first.
    ///
    /// Only ever consulted when the caller supplied no explicit `refName`, and
    /// a candidate that does not resolve is skipped rather than fatal — see
    /// [`GitHubClient::resolve_github_ref`].
    url_ref_candidates: Vec<String>,
}

/// Path segments that introduce a ref in a github.com URL. `commits` (plural)
/// is the history view, `commit` (singular) a single commit; both are followed
/// by a ref.
const GITHUB_REF_MARKERS: [&str; 4] = ["tree", "blob", "commit", "commits"];

/// Turns the segments after a ref marker into resolution candidates.
///
/// `/tree/<ref>/<path>` is genuinely ambiguous: `main/src` is either branch
/// `main` plus directory `src`, or a branch literally named `main/src` (git
/// allows slashes, and `release/1.x` is a common shape). Nothing in the URL
/// distinguishes them — GitHub's own web UI resolves it server-side against the
/// real ref list, which is why `extension/src/content/detect.js` reads the ref
/// from the page instead of the path.
///
/// So both readings are offered, longest first: the whole remainder, then its
/// first segment. That covers a slashed ref with no subpath and a simple ref
/// with a subpath — the two shapes that actually occur. A slashed ref *and* a
/// subpath resolves to neither and falls through to the default branch, which
/// is what this code did for every one of these URLs before.
fn ref_candidates_from_path(rest: &[String]) -> Vec<String> {
    if rest.is_empty() {
        return Vec::new();
    }
    // `Url::path_segments` yields percent-encoded segments, but a ref name
    // travels onward as data that `resolve_commit` encodes again. Leaving the
    // escapes in place turned `release%2020.1` into `release%252020.1`, which
    // GitHub answers with a 404. A bare `%` is not an error here: `100%` is a
    // legal branch name, so a segment that does not decode is kept verbatim.
    let decoded: Vec<String> = rest
        .iter()
        .map(|part| match urlencoding::decode(part) {
            Ok(value) => value.into_owned(),
            Err(_) => part.clone(),
        })
        .collect();
    let whole = decoded.join("/");
    let first = decoded[0].clone();
    if first == whole {
        vec![whole]
    } else {
        vec![whole, first]
    }
}

fn build_ref_cache(ttl: Duration) -> Cache<(String, Option<String>), RepoRef> {
    Cache::builder()
        .max_capacity(REF_CACHE_CAPACITY)
        .time_to_live(ttl)
        .eviction_policy(EvictionPolicy::lru())
        .build()
}

/// Star counts drift continuously, so the refresh cache is short; failures
/// (None) are cached too, so an unreachable API does not become a request
/// amplifier.
fn build_stars_cache() -> Cache<(RepositoryProvider, String, String), Option<u64>> {
    Cache::builder()
        .max_capacity(REF_CACHE_CAPACITY)
        .time_to_live(Duration::from_secs(600))
        .eviction_policy(EvictionPolicy::lru())
        .build()
}

impl GitHubClient {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_token(std::env::var("GITHUB_TOKEN").ok())
    }

    /// Explicit-token constructor so tests can build both a tokened and an
    /// untokened client without racing on the process environment.
    fn with_token(token: Option<String>) -> anyhow::Result<Self> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static("octocount-service/0.1"),
        );
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/vnd.github+json"),
        );

        let token = token.filter(|token| !token.trim().is_empty());
        let has_token = token.is_some();
        if let Some(token) = token {
            let value = format!("Bearer {token}");
            headers.insert(
                header::AUTHORIZATION,
                header::HeaderValue::from_str(&value)?,
            );
        }

        Ok(Self {
            // JSON-sized calls: connect budget plus an overall budget well
            // above a normal API round trip but short enough that a wedged
            // upstream fails fast instead of pinning a worker.
            client: Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .default_headers(headers.clone())
                .build()?,
            // The archive path streams bodies that can run to hundreds of
            // megabytes, so it gets a much longer overall budget — bounded,
            // but generous enough that a slow link on a large repo is not
            // cut off mid-download.
            archive_client: Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(600))
                .default_headers(headers)
                .build()?,
            ref_cache: build_ref_cache(REF_CACHE_TTL),
            stars_cache: build_stars_cache(),
            has_token,
            rate_limited_429: Arc::new(AtomicU64::new(0)),
        })
    }

    /// How many 429 responses have been seen from upstream hosts so far.
    pub fn rate_limited_429_count(&self) -> u64 {
        self.rate_limited_429.load(Ordering::Relaxed)
    }

    /// GET with retries on transient upstream failures (5xx, connect and
    /// timeout errors). Non-retryable transport errors stay
    /// [`GitHubError::Request`]; everything retryable that survives the
    /// schedule becomes [`GitHubError::UpstreamUnavailable`].
    ///
    /// The retry engine is shared by JSON calls and the archive stream: only
    /// the request/response handshake is retried. Once a 200 body starts
    /// streaming it is never restarted — a mid-stream failure surfaces as a
    /// read error on the consumer side instead.
    async fn get_with_retry(
        &self,
        url: &str,
        long_running: bool,
    ) -> Result<reqwest::Response, GitHubError> {
        let client = if long_running {
            &self.archive_client
        } else {
            &self.client
        };
        let mut delays = UPSTREAM_RETRY_DELAYS.iter();
        // A 429 is retried (during incidents codeload sheds load
        // nondeterministically, so the next attempt often passes), but if the
        // limit holds the response is returned untouched so callers classify
        // it as rate limiting rather than an outage. When the response says
        // how long the limit lasts, that — not the fixed backoff schedule —
        // decides the wait.
        let mut last_rate_limited: Option<reqwest::Response> = None;
        let mut rate_limit_sleeps = 0;
        loop {
            match client.get(url).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_server_error() {
                        tracing::warn!(%status, "upstream 5xx; retrying");
                    } else if status == StatusCode::TOO_MANY_REQUESTS {
                        self.rate_limited_429.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%status, "upstream rate limited; retrying");
                        match rate_limit_wait(&response) {
                            // The limit lifts later than we are willing to
                            // wait: hand the 429 back for the caller to
                            // classify instead of parking the request.
                            Some(wait) if wait > RATE_LIMIT_SLEEP_CAP => {
                                tracing::warn!(?wait, "upstream rate limit outlives the wait cap");
                                return Ok(response);
                            }
                            Some(wait) => {
                                if rate_limit_sleeps >= MAX_RATE_LIMIT_SLEEPS {
                                    return Ok(response);
                                }
                                rate_limit_sleeps += 1;
                                tracing::warn!(?wait, "sleeping for upstream rate limit");
                                tokio::time::sleep(wait).await;
                                continue;
                            }
                            // No advisory headers: fall back to the fixed
                            // backoff schedule like a 5xx.
                            None => last_rate_limited = Some(response),
                        }
                    } else {
                        return Ok(response);
                    }
                }
                Err(error) => {
                    if error.is_timeout() || error.is_connect() {
                        tracing::warn!(%error, "upstream request failed; retrying");
                    } else {
                        return Err(GitHubError::Request(error));
                    }
                }
            };
            match delays.next() {
                Some(delay) => tokio::time::sleep(*delay).await,
                None => match last_rate_limited {
                    Some(response) => return Ok(response),
                    None => return Err(GitHubError::UpstreamUnavailable),
                },
            }
        }
    }

    /// Current star count for a repository, cached briefly so share-card
    /// refreshes do not hammer the GitHub API. `None` means unknown (missing
    /// field, rate limit, or transport failure) — never zero.
    pub async fn repo_stars(
        &self,
        provider: &RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Option<u64> {
        let cache_key = (provider.clone(), owner.to_string(), repo.to_string());
        if let Some(cached) = self.stars_cache.get(&cache_key).await {
            return cached;
        }

        let stars = match provider {
            RepositoryProvider::GitHub => {
                let url = format!(
                    "https://api.github.com/repos/{}/{}",
                    urlencoding::encode(owner),
                    urlencoding::encode(repo)
                );
                let response = self.get_with_retry(&url, false).await.ok()?;
                if !matches!(response.status(), StatusCode::OK) {
                    return None;
                }
                response
                    .json::<RepoResponse>()
                    .await
                    .ok()?
                    .stargazers_count
            }
            RepositoryProvider::GitLab => {
                let full_path = format!("{owner}/{repo}");
                let path = urlencoding::encode(&full_path);
                let url = format!("https://gitlab.com/api/v4/projects/{path}");
                let response = self.get_with_retry(&url, false).await.ok()?;
                if !matches!(response.status(), StatusCode::OK) {
                    return None;
                }
                response
                    .json::<GitLabProjectResponse>()
                    .await
                    .ok()?
                    .star_count
            }
        };

        self.stars_cache.insert(cache_key, stars).await;
        stars
    }

    fn parse_repo_url(input: &str) -> Result<RepoTarget, GitHubError> {
        let mut normalized = input.trim().to_string();
        if normalized.starts_with("git@github.com:") {
            normalized = normalized.replacen("git@github.com:", "https://github.com/", 1);
        } else if normalized.starts_with("git@gitlab.com:") {
            normalized = normalized.replacen("git@gitlab.com:", "https://gitlab.com/", 1);
        }

        let url = Url::parse(&normalized).map_err(|_| GitHubError::InvalidUrl)?;
        let host = url.host_str().ok_or(GitHubError::InvalidUrl)?;

        let segments: Vec<String> = url
            .path_segments()
            .ok_or(GitHubError::InvalidUrl)?
            .filter(|part| !part.is_empty())
            .map(|part| part.trim_end_matches(".git").to_string())
            .collect();

        if segments.len() < 2 {
            return Err(GitHubError::InvalidUrl);
        }

        match host {
            "github.com" => {
                let owner = segments[0].clone();
                let repo = segments[1].clone();
                if owner.is_empty() || repo.is_empty() {
                    return Err(GitHubError::InvalidUrl);
                }
                // A pasted browse URL already says which ref the user is looking
                // at. Segments past the marker used to be discarded, so
                // `/tree/master` silently analysed the default branch — harmless
                // when that is `main`, wrong for every repo where it is not.
                let url_ref_candidates = match segments.get(2) {
                    Some(marker) if GITHUB_REF_MARKERS.contains(&marker.as_str()) => {
                        ref_candidates_from_path(&segments[3..])
                    }
                    _ => Vec::new(),
                };
                Ok(RepoTarget {
                    provider: RepositoryProvider::GitHub,
                    path: format!("{owner}/{repo}"),
                    owner,
                    repo,
                    url_ref_candidates,
                })
            }
            "gitlab.com" => {
                let repo = segments.last().cloned().ok_or(GitHubError::InvalidUrl)?;
                let owner = segments[..segments.len() - 1].join("/");
                let path = segments.join("/");
                if owner.is_empty() || repo.is_empty() {
                    return Err(GitHubError::InvalidUrl);
                }
                Ok(RepoTarget {
                    provider: RepositoryProvider::GitLab,
                    owner,
                    repo,
                    path,
                    // GitLab browse URLs put the ref behind a `/-/` separator that
                    // this parser does not split on yet, so there is nothing
                    // trustworthy to extract here.
                    url_ref_candidates: Vec::new(),
                })
            }
            _ => Err(GitHubError::InvalidUrl),
        }
    }

    /// Keeps the token (so rate limits stay generous) but disarms the GraphQL
    /// fast path, so a test can run the same resolution down both routes.
    #[cfg(test)]
    fn without_graphql(mut self) -> Self {
        self.has_token = false;
        self
    }

    #[cfg(test)]
    pub fn parse_repo_owner_name(input: &str) -> Result<(String, String), GitHubError> {
        let target = Self::parse_repo_url(input)?;
        Ok((target.owner, target.repo))
    }

    pub async fn resolve_ref(
        &self,
        repo_url: &str,
        requested_ref: Option<String>,
        bypass_cache: bool,
    ) -> Result<RepoRef, GitHubError> {
        let cache_key = (repo_url.to_owned(), requested_ref.clone());
        if !bypass_cache {
            if let Some(cached) = self.ref_cache.get(&cache_key).await {
                return Ok(cached);
            }
        }

        let target = Self::parse_repo_url(repo_url)?;
        match target.provider {
            RepositoryProvider::GitHub => {
                self.resolve_github_ref(target, requested_ref, cache_key)
                    .await
            }
            RepositoryProvider::GitLab => {
                self.resolve_gitlab_ref(target, requested_ref, cache_key)
                    .await
            }
        }
    }

    /// GitHub ref resolution, with the pasted URL's own ref as a fallback source.
    ///
    /// An explicit `requested_ref` is authoritative and resolved exactly once: a
    /// caller who names a ref that does not exist should be told so, not quietly
    /// given a different one. Only when no ref was supplied are the URL-derived
    /// candidates tried, and a candidate that turns out not to be a ref is
    /// skipped rather than fatal — the last resort is still the default branch,
    /// which is what every browse URL resolved to before, so no URL that used to
    /// work can start failing.
    async fn resolve_github_ref(
        &self,
        target: RepoTarget,
        requested_ref: Option<String>,
        cache_key: (String, Option<String>),
    ) -> Result<RepoRef, GitHubError> {
        let requested_ref = requested_ref.filter(|value| !value.trim().is_empty());
        if requested_ref.is_some() || target.url_ref_candidates.is_empty() {
            return self
                .resolve_github_ref_exact(&target.owner, &target.repo, requested_ref, cache_key)
                .await;
        }

        for candidate in &target.url_ref_candidates {
            match self
                .resolve_github_ref_exact(
                    &target.owner,
                    &target.repo,
                    Some(candidate.clone()),
                    cache_key.clone(),
                )
                .await
            {
                // Only "that is not a ref" means try the next reading of the
                // path. Every other error is about the repository itself and
                // would come back identically from the remaining attempts.
                Err(GitHubError::RefNotFound { .. }) => continue,
                other => return other,
            }
        }

        self.resolve_github_ref_exact(&target.owner, &target.repo, None, cache_key)
            .await
    }

    /// Resolves one ref, or the default branch when `requested_ref` is `None`.
    /// The GraphQL fast path answers in a single request; REST needs two and is
    /// also the fallback whenever GraphQL cannot give a definitive answer.
    async fn resolve_github_ref_exact(
        &self,
        owner: &str,
        repo: &str,
        requested_ref: Option<String>,
        cache_key: (String, Option<String>),
    ) -> Result<RepoRef, GitHubError> {
        let owner = owner.to_owned();
        let repo = repo.to_owned();

        if self.has_token {
            match self
                .resolve_github_ref_graphql(&owner, &repo, requested_ref.as_deref())
                .await
            {
                GraphQlOutcome::Resolved {
                    html_url,
                    ref_name,
                    stars,
                    commit_sha,
                } => {
                    let repo_ref = RepoRef {
                        provider: RepositoryProvider::GitHub,
                        owner,
                        repo,
                        ref_name,
                        commit_sha,
                        html_url,
                        stars,
                    };
                    self.ref_cache.insert(cache_key, repo_ref.clone()).await;
                    return Ok(repo_ref);
                }
                GraphQlOutcome::Failed(failure) => return Err(failure.into()),
                GraphQlOutcome::Unusable => {}
            }
        }

        let repo_api = format!(
            "https://api.github.com/repos/{}/{}",
            urlencoding::encode(&owner),
            urlencoding::encode(&repo)
        );
        let repo_response = self.get_with_retry(&repo_api, false).await?;
        match repo_response.status() {
            StatusCode::OK => {}
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => {
                return Err(GitHubError::RateLimited)
            }
            StatusCode::NOT_FOUND => return Err(GitHubError::NotFound),
            status if status.is_server_error() => {
                return Err(GitHubError::UpstreamUnavailable)
            }
            _ => return Err(GitHubError::NotFound),
        }
        let repo_body: RepoResponse = repo_response.json().await?;
        if repo_body.private {
            return Err(GitHubError::PrivateRepo);
        }

        let default_branch = Some(repo_body.default_branch.clone()).filter(|name| !name.is_empty());
        let used_default = requested_ref.is_none();
        let ref_name = match requested_ref {
            Some(value) => value,
            // Mirrors the GraphQL path: with no ref requested and no default
            // branch to fall back to, the repository is empty. Reporting that as
            // a missing ref is what sends people looking for a ref field to fix.
            None => default_branch.clone().ok_or(GitHubError::EmptyRepository)?,
        };

        let commit_sha = match self.resolve_commit(&owner, &repo, &ref_name).await {
            Ok(sha) => sha,
            // A 404 on the branch the repo call just named as the default means
            // the branch exists as a setting but has no commits.
            Err(GitHubError::RefNotFound { .. }) if used_default => {
                return Err(GitHubError::EmptyRepository)
            }
            // `resolve_commit` cannot know the default branch; this call site
            // does, and attaching it here is what lets the API name the branch
            // the caller probably wanted.
            Err(GitHubError::RefNotFound { .. }) => {
                return Err(GitHubError::RefNotFound { default_branch })
            }
            Err(other) => return Err(other),
        };

        let repo_ref = RepoRef {
            provider: RepositoryProvider::GitHub,
            owner,
            repo,
            ref_name,
            commit_sha,
            html_url: repo_body.html_url,
            stars: repo_body.stargazers_count,
        };
        self.ref_cache.insert(cache_key, repo_ref.clone()).await;
        Ok(repo_ref)
    }

    /// Single-request ref resolution. Never returns a transport error: a GraphQL
    /// request that does not produce a definitive answer resolves to
    /// [`GraphQlOutcome::Unusable`] and the caller retries over REST, which is
    /// the path that then reports the failure.
    async fn resolve_github_ref_graphql(
        &self,
        owner: &str,
        repo: &str,
        requested_ref: Option<&str>,
    ) -> GraphQlOutcome {
        let body = serde_json::json!({
            "query": REF_QUERY,
            "variables": {
                "owner": owner,
                "name": repo,
                // GraphQL requires a value for a non-null variable even when the
                // field using it is skipped by @include.
                "expression": requested_ref.unwrap_or("HEAD"),
                "hasRef": requested_ref.is_some(),
            },
        });

        let response = match self.client.post(GRAPHQL_ENDPOINT).json(&body).send().await {
            Ok(response) => response,
            Err(error) => {
                tracing::debug!(%error, "GraphQL ref resolution failed; falling back to REST");
                return GraphQlOutcome::Unusable;
            }
        };

        match response.status() {
            StatusCode::OK => {}
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => {
                return GraphQlOutcome::Failed(GraphQlFailure::RateLimited)
            }
            // 401 from a bad or expired token, 5xx from GitHub: REST decides.
            status => {
                tracing::debug!(%status, "unexpected GraphQL status; falling back to REST");
                return GraphQlOutcome::Unusable;
            }
        }

        match response.json::<GraphQlResponse>().await {
            Ok(body) => interpret_graphql(body, requested_ref),
            Err(error) => {
                tracing::debug!(%error, "unreadable GraphQL body; falling back to REST");
                GraphQlOutcome::Unusable
            }
        }
    }

    async fn resolve_gitlab_ref(
        &self,
        target: RepoTarget,
        requested_ref: Option<String>,
        cache_key: (String, Option<String>),
    ) -> Result<RepoRef, GitHubError> {
        let encoded_path = urlencoding::encode(&target.path);
        let project_api = format!("https://gitlab.com/api/v4/projects/{encoded_path}");
        let project_response = self.get_with_retry(&project_api, false).await?;
        match project_response.status() {
            StatusCode::OK => {}
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => {
                return Err(GitHubError::RateLimited)
            }
            StatusCode::NOT_FOUND => return Err(GitHubError::NotFound),
            status if status.is_server_error() => {
                return Err(GitHubError::UpstreamUnavailable)
            }
            _ => return Err(GitHubError::NotFound),
        }
        let project_body: GitLabProjectResponse = project_response.json().await?;
        if project_body.visibility != "public" {
            return Err(GitHubError::PrivateRepo);
        }

        let requested_ref = requested_ref.filter(|value| !value.trim().is_empty());
        let used_default = requested_ref.is_none();
        let default_branch = project_body
            .default_branch
            .clone()
            .filter(|name| !name.trim().is_empty());
        let ref_name = match requested_ref {
            Some(value) => value,
            // Nothing to analyse and nothing to suggest. Mirrors the GitHub
            // path, which reaches the same conclusion without `empty_repo`.
            None if project_body.empty_repo => return Err(GitHubError::EmptyRepository),
            // No branch to fall back to on a project that claims to have
            // commits: GitLab withheld the field because we lack `read_code`,
            // so the archive endpoint would refuse us too. `empty_repository`
            // used to be the answer here, which told the user their repository
            // has no commits — confidently false about a repository whose code
            // is merely hidden. Only this arm checks it: a caller that named
            // its own ref needs no default branch, and a schema change that
            // drops the field must not be able to reject that request.
            None => default_branch.clone().ok_or(GitHubError::PrivateRepo)?,
        };
        let commit_sha = match self.resolve_gitlab_commit(project_body.id, &ref_name).await {
            Ok(sha) => sha,
            // The default branch itself has no commits, so the ref was never
            // the problem.
            Err(GitHubError::RefNotFound { .. }) if used_default => {
                return Err(GitHubError::EmptyRepository)
            }
            // Same reason as the GitHub path: the project request already told
            // us the default branch, so the 404 can name it.
            Err(GitHubError::RefNotFound { .. }) => {
                return Err(GitHubError::RefNotFound { default_branch })
            }
            Err(other) => return Err(other),
        };
        let owner = project_body
            .path_with_namespace
            .rsplit_once('/')
            .map(|(owner, _)| owner.to_string())
            .unwrap_or_else(|| target.owner.clone());
        let repo_ref = RepoRef {
            provider: RepositoryProvider::GitLab,
            owner,
            repo: target.repo,
            ref_name,
            commit_sha,
            html_url: project_body.web_url,
            stars: project_body.star_count,
        };
        self.ref_cache.insert(cache_key, repo_ref.clone()).await;
        Ok(repo_ref)
    }

    /// Returns `RefNotFound` with no default branch: this call does not know it.
    /// The caller has it from the preceding repository request and attaches it.
    async fn resolve_commit(
        &self,
        owner: &str,
        repo: &str,
        ref_name: &str,
    ) -> Result<String, GitHubError> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/commits/{}",
            urlencoding::encode(owner),
            urlencoding::encode(repo),
            urlencoding::encode(ref_name)
        );
        let response = self.get_with_retry(&url, false).await?;
        match response.status() {
            StatusCode::OK => {
                let body: CommitResponse = response.json().await?;
                Ok(body.sha)
            }
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => Err(GitHubError::RateLimited),
            StatusCode::NOT_FOUND => Err(GitHubError::RefNotFound {
                default_branch: None,
            }),
            // "Git Repository is empty." GitHub answers this endpoint with a 409
            // for a repository with no commits, whatever ref was asked for. It
            // used to fall into the catch-all below and be reported as a missing
            // ref alongside the default branch from `/repos` — a branch that has
            // no commits either. GraphQL calls the same state `EmptyRepository`,
            // and the two paths have to agree.
            StatusCode::CONFLICT => Err(GitHubError::EmptyRepository),
            status if status.is_server_error() => Err(GitHubError::UpstreamUnavailable),
            _ => Err(GitHubError::RefNotFound {
                default_branch: None,
            }),
        }
    }

    async fn resolve_gitlab_commit(
        &self,
        project_id: u64,
        ref_name: &str,
    ) -> Result<String, GitHubError> {
        let encoded_ref = urlencoding::encode(ref_name);
        let url = format!(
            "https://gitlab.com/api/v4/projects/{project_id}/repository/commits/{encoded_ref}"
        );
        let response = self.get_with_retry(&url, false).await?;
        match response.status() {
            StatusCode::OK => {
                let body: GitLabCommitResponse = response.json().await?;
                Ok(body.id)
            }
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => Err(GitHubError::RateLimited),
            StatusCode::NOT_FOUND => Err(GitHubError::RefNotFound {
                default_branch: None,
            }),
            status if status.is_server_error() => Err(GitHubError::UpstreamUnavailable),
            _ => Err(GitHubError::RefNotFound {
                default_branch: None,
            }),
        }
    }

    /// Opens the repository archive as a blocking reader instead of buffering it.
    ///
    /// The returned reader must be consumed on a blocking thread
    /// (`spawn_blocking`), which is where the analyzer already does its work. It
    /// pulls from the live HTTP response, so extraction proceeds while the rest
    /// of the archive is still arriving and the process never holds more than a
    /// buffer's worth of the tarball.
    ///
    /// Only the response head is inspected here. The hard size limit is applied
    /// by the analyzer as it reads, because that is where the byte budget and
    /// the temp directory live; `max_bytes` is used solely for the free
    /// `Content-Length` early-out, which codeload usually does not offer.
    pub async fn archive_reader(
        &self,
        provider: &RepositoryProvider,
        owner: &str,
        repo: &str,
        sha: &str,
        max_bytes: u64,
    ) -> Result<Box<dyn std::io::Read + Send>, GitHubError> {
        let url = match provider {
            RepositoryProvider::GitHub => {
                format!(
                    "https://codeload.github.com/{}/{}/tar.gz/{}",
                    urlencoding::encode(owner),
                    urlencoding::encode(repo),
                    urlencoding::encode(sha)
                )
            }
            RepositoryProvider::GitLab => {
                let path = format!("{owner}/{repo}");
                let encoded_path = urlencoding::encode(&path);
                format!("https://gitlab.com/api/v4/projects/{encoded_path}/repository/archive.tar.gz?sha={sha}")
            }
        };
        self.stream_url(url, max_bytes).await
    }

    /// Test access to [`Self::stream_url`], so the streaming path can be
    /// exercised against a local server rather than codeload.
    #[cfg(test)]
    pub async fn stream_url_for_test(
        &self,
        url: String,
        max_bytes: u64,
    ) -> Result<Box<dyn std::io::Read + Send>, GitHubError> {
        self.stream_url(url, max_bytes).await
    }

    /// The transport half of [`Self::archive_reader`], split out so tests can
    /// point it at a local server without going through codeload.
    async fn stream_url(
        &self,
        url: String,
        max_bytes: u64,
    ) -> Result<Box<dyn std::io::Read + Send>, GitHubError> {
        let response = self.get_with_retry(&url, true).await?;
        match response.status() {
            StatusCode::OK => {}
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => {
                return Err(GitHubError::RateLimited)
            }
            StatusCode::NOT_FOUND => return Err(GitHubError::NotFound),
            _ => {
                return Err(GitHubError::Request(
                    response.error_for_status().unwrap_err(),
                ))
            }
        }

        if let Some(length) = response.content_length() {
            if length > max_bytes {
                return Err(GitHubError::TooLarge);
            }
        }

        let stream = response.bytes_stream().map_err(std::io::Error::other);
        Ok(Box::new(SyncIoBridge::new(StreamReader::new(stream))))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_ref_cache, interpret_graphql, GitHubClient, GitHubError, GitLabProjectResponse,
        GraphQlFailure, GraphQlOutcome, RepoRef, RepositoryProvider, REF_CACHE_TTL,
    };
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::response::IntoResponse;

    /// Local axum server counting requests, answering 503 until `ok_after`
    /// requests have arrived, then 200. Lets `get_with_retry` be exercised
    /// end to end without GitHub or codeload.
    /// Variant of [`flaky_server`] answering 429 instead of 503, mirroring
    /// how codeload sheds load during incidents.
    async fn throttled_server(ok_after: u32) -> (String, Arc<AtomicU32>) {
        throttled_server_with_headers(ok_after, None).await
    }

    /// [`throttled_server`], optionally stamping a `retry-after` header on the
    /// 429s so the header-aware wait can be exercised.
    async fn throttled_server_with_headers(
        ok_after: u32,
        retry_after: Option<&'static str>,
    ) -> (String, Arc<AtomicU32>) {
        use axum::routing::get;
        let hits = Arc::new(AtomicU32::new(0));
        let state = hits.clone();
        let app = axum::Router::new().route(
            "/repos/x",
            get(move || {
                let seen = state.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if seen >= ok_after {
                        axum::Json(serde_json::json!({"stargazers_count": 42})).into_response()
                    } else {
                        let mut response =
                            (axum::http::StatusCode::TOO_MANY_REQUESTS, "slow down")
                                .into_response();
                        if let Some(value) = retry_after {
                            response.headers_mut().insert(
                                "retry-after",
                                axum::http::HeaderValue::from_static(value),
                            );
                        }
                        response
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/repos/x"), hits)
    }

    #[tokio::test]
    async fn get_with_retry_survives_a_transient_429() {
        let (url, hits) = throttled_server(2).await; // 429, then 200
        let client = GitHubClient::with_token(None).unwrap();
        let response = client.get_with_retry(&url, false).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn get_with_retry_returns_the_429_when_the_limit_holds() {
        let (url, hits) = throttled_server(u32::MAX).await; // always 429
        let client = GitHubClient::with_token(None).unwrap();
        let response = client.get_with_retry(&url, false).await.unwrap();
        assert_eq!(response.status().as_u16(), 429);
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    /// A `retry-after` longer than the wait cap must not park the request:
    /// the 429 comes back on the first attempt for the caller to classify.
    #[tokio::test]
    async fn a_long_retry_after_is_returned_instead_of_waited_out() {
        let (url, hits) = throttled_server_with_headers(u32::MAX, Some("120")).await;
        let client = GitHubClient::with_token(None).unwrap();
        let response = client.get_with_retry(&url, false).await.unwrap();
        assert_eq!(response.status().as_u16(), 429);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    /// A zero-second `retry-after` still retries, but only a bounded number
    /// of times before the 429 is surfaced.
    #[tokio::test]
    async fn a_zero_retry_after_is_honoured_but_bounded() {
        let (url, hits) = throttled_server_with_headers(u32::MAX, Some("0")).await;
        let client = GitHubClient::with_token(None).unwrap();
        let response = client.get_with_retry(&url, false).await.unwrap();
        assert_eq!(response.status().as_u16(), 429);
        // initial attempt + MAX_RATE_LIMIT_SLEEPS header-directed retries
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_short_retry_after_lets_the_retry_succeed() {
        let (url, hits) = throttled_server_with_headers(2, Some("0")).await; // 429, then 200
        let client = GitHubClient::with_token(None).unwrap();
        let response = client.get_with_retry(&url, false).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    async fn flaky_server(ok_after: u32) -> (String, Arc<AtomicU32>) {
        use axum::routing::get;
        let hits = Arc::new(AtomicU32::new(0));
        let state = hits.clone();
        let app = axum::Router::new().route(
            "/repos/x",
            get(move || {
                let seen = state.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if seen >= ok_after {
                        axum::Json(serde_json::json!({"stargazers_count": 42})).into_response()
                    } else {
                        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "down").into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/repos/x"), hits)
    }

    #[tokio::test]
    async fn get_with_retry_rides_out_transient_5xx() {
        let (url, hits) = flaky_server(3).await; // 503, 503, then 200
        let client = GitHubClient::with_token(None).unwrap();
        let response = client.get_with_retry(&url, false).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn get_with_retry_reports_upstream_unavailable_after_backoff() {
        let (url, hits) = flaky_server(u32::MAX).await; // always 503
        let client = GitHubClient::with_token(None).unwrap();
        let error = client.get_with_retry(&url, false).await.unwrap_err();
        assert!(matches!(error, super::GitHubError::UpstreamUnavailable));
        // initial attempt + both backoff retries
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    fn sample_ref(sha: &str) -> RepoRef {
        RepoRef {
            provider: RepositoryProvider::GitHub,
            owner: "tokio-rs".to_string(),
            repo: "axum".to_string(),
            ref_name: "main".to_string(),
            commit_sha: sha.to_string(),
            html_url: "https://github.com/tokio-rs/axum".to_string(),
            stars: None,
        }
    }

    /// A moving branch must eventually be re-resolved without `force_refresh`,
    /// which is only true if the cache carries an expiry at all.
    #[test]
    fn ref_cache_has_a_bounded_lifetime() {
        assert!(REF_CACHE_TTL > Duration::ZERO);
        assert!(REF_CACHE_TTL <= Duration::from_secs(300));
    }

    #[tokio::test]
    async fn ref_cache_entries_expire_so_moved_branches_are_re_resolved() {
        let cache = build_ref_cache(Duration::from_millis(50));
        let key = ("https://github.com/tokio-rs/axum".to_string(), None);

        cache.insert(key.clone(), sample_ref("old")).await;
        assert_eq!(
            cache.get(&key).await.map(|value| value.commit_sha),
            Some("old".to_string())
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        cache.run_pending_tasks().await;

        assert!(
            cache.get(&key).await.is_none(),
            "expired entry was still served; the branch would stay pinned to a stale sha"
        );
    }

    // ---------------------------------------------------------------------
    // GraphQL ref resolution
    //
    // The bodies below are the shapes the live API returned while this was
    // built, trimmed to the fields the query selects. The commit shas were
    // cross-checked against `GET /repos/{o}/{r}/commits/{ref}` for a moving
    // branch, a lightweight tag, an annotated tag (torvalds/linux v6.6, where a
    // naive `object(expression:)` reading could have produced the tag object's
    // own sha instead of the commit's), a raw sha, and a renamed repository.
    // ---------------------------------------------------------------------

    fn interpret(body: &str, requested_ref: Option<&str>) -> GraphQlOutcome {
        interpret_graphql(serde_json::from_str(body).unwrap(), requested_ref)
    }

    #[test]
    fn graphql_resolves_the_default_branch_when_no_ref_is_requested() {
        let outcome = interpret(
            r#"{"data":{"repository":{
                "isPrivate":false,
                "url":"https://github.com/tokio-rs/axum",
                "defaultBranchRef":{"name":"main","target":{"oid":"a5116d6b1bcabdfd7039279e4957b4a9c0b50587"}}
            }}}"#,
            None,
        );
        assert_eq!(
            outcome,
            GraphQlOutcome::Resolved {
                html_url: "https://github.com/tokio-rs/axum".to_string(),
                ref_name: "main".to_string(),
                stars: None,
                commit_sha: "a5116d6b1bcabdfd7039279e4957b4a9c0b50587".to_string(),
            }
        );
    }

    /// `v6.6` is an annotated tag. GitHub peels it to the commit, which is the
    /// same sha REST reports; anything else would silently fork the report cache
    /// and point the archive download at a tag object.
    #[test]
    fn graphql_resolves_an_annotated_tag_to_its_commit() {
        let outcome = interpret(
            r#"{"data":{"repository":{
                "isPrivate":false,
                "url":"https://github.com/torvalds/linux",
                "defaultBranchRef":{"name":"master","target":{"oid":"cd78d08026c75c6681c2e5e418aad800e729d54d"}},
                "object":{"__typename":"Commit","oid":"ffc253263a1375a65fa6c9f62a893e9767fbebfa"}
            }}}"#,
            Some("v6.6"),
        );
        assert_eq!(
            outcome,
            GraphQlOutcome::Resolved {
                html_url: "https://github.com/torvalds/linux".to_string(),
                ref_name: "v6.6".to_string(),
                stars: None,
                commit_sha: "ffc253263a1375a65fa6c9f62a893e9767fbebfa".to_string(),
            }
        );
    }

    /// A renamed repository resolves under its old name and reports the new
    /// canonical URL, matching REST's redirect behaviour.
    #[test]
    fn graphql_reports_the_canonical_url_for_a_renamed_repository() {
        let outcome = interpret(
            r#"{"data":{"repository":{
                "isPrivate":false,
                "url":"https://github.com/vuejs/core",
                "defaultBranchRef":{"name":"main","target":{"oid":"b5f8518379b77c3b62a7a9d2b52f6c76cda09bd5"}}
            }}}"#,
            None,
        );
        let GraphQlOutcome::Resolved { html_url, .. } = outcome else {
            panic!("expected a resolution");
        };
        assert_eq!(html_url, "https://github.com/vuejs/core");
    }

    #[test]
    fn graphql_rejects_private_repositories() {
        let outcome = interpret(
            r#"{"data":{"repository":{
                "isPrivate":true,
                "url":"https://github.com/acme/secret",
                "defaultBranchRef":{"name":"main","target":{"oid":"aaaa"}}
            }}}"#,
            None,
        );
        assert_eq!(
            outcome,
            GraphQlOutcome::Failed(GraphQlFailure::PrivateRepo)
        );
    }

    #[test]
    fn graphql_maps_a_missing_repository_to_not_found() {
        let outcome = interpret(
            r#"{"data":{"repository":null},"errors":[{"type":"NOT_FOUND","message":"Could not resolve to a Repository"}]}"#,
            None,
        );
        assert_eq!(outcome, GraphQlOutcome::Failed(GraphQlFailure::NotFound));
    }

    #[test]
    fn graphql_maps_rate_limiting_to_rate_limited() {
        let outcome = interpret(
            r#"{"data":null,"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}"#,
            None,
        );
        assert_eq!(outcome, GraphQlOutcome::Failed(GraphQlFailure::RateLimited));
    }

    /// A ref that resolves to nothing comes back as a present repository with a
    /// null object. The failure must carry the repository's real default branch:
    /// that is the only thing that tells a caller who asked for `main` on a
    /// `master` repository what to ask for instead.
    #[test]
    fn graphql_maps_an_unresolvable_ref_to_ref_not_found() {
        let outcome = interpret(
            r#"{"data":{"repository":{
                "isPrivate":false,
                "url":"https://github.com/tokio-rs/axum",
                "defaultBranchRef":{"name":"master","target":{"oid":"a511"}},
                "object":null
            }}}"#,
            Some("does-not-exist-ref"),
        );
        assert_eq!(
            outcome,
            GraphQlOutcome::Failed(GraphQlFailure::RefNotFound {
                default_branch: Some("master".to_string()),
            })
        );
    }

    /// An empty repository has no default branch to resolve — and no ref was
    /// requested, so it must not be reported as a missing ref.
    #[test]
    fn graphql_maps_an_empty_repository_to_empty_repository() {
        let outcome = interpret(
            r#"{"data":{"repository":{
                "isPrivate":false,
                "url":"https://github.com/acme/empty",
                "defaultBranchRef":null
            }}}"#,
            None,
        );
        assert_eq!(
            outcome,
            GraphQlOutcome::Failed(GraphQlFailure::EmptyRepository)
        );
    }

    /// A requested ref that resolves to nothing in a repository that also has no
    /// default branch is the empty-repository case, not a missing ref: there is
    /// no ref the caller could have asked for instead. This used to answer
    /// `ref_not_found` with no branch to suggest, which REST — reading a 409
    /// off the commits endpoint — contradicted for the same request.
    #[test]
    fn graphql_reports_an_empty_repository_even_when_a_ref_was_requested() {
        let outcome = interpret(
            r#"{"data":{"repository":{
                "isPrivate":false,
                "url":"https://github.com/acme/empty",
                "defaultBranchRef":null,
                "object":null
            }}}"#,
            Some("master"),
        );
        assert_eq!(
            outcome,
            GraphQlOutcome::Failed(GraphQlFailure::EmptyRepository)
        );
    }

    /// The safety valve. An object type this code does not understand, or an
    /// error class it has no mapping for, must fall through to REST rather than
    /// be guessed at.
    #[test]
    fn graphql_defers_to_rest_for_shapes_it_does_not_recognise() {
        assert_eq!(
            interpret(
                r#"{"data":{"repository":{
                    "isPrivate":false,
                    "url":"https://github.com/acme/blobref",
                    "defaultBranchRef":{"name":"main","target":{"oid":"aaaa"}},
                    "object":{"__typename":"Blob","oid":"bbbb"}
                }}}"#,
                Some("some/file.txt"),
            ),
            GraphQlOutcome::Unusable
        );

        assert_eq!(
            interpret(
                r#"{"data":{"repository":null},"errors":[{"type":"SOMETHING_NEW"}]}"#,
                None,
            ),
            GraphQlOutcome::Unusable
        );

        assert_eq!(
            interpret(r#"{"data":null,"errors":[{"message":"no type field"}]}"#, None),
            GraphQlOutcome::Unusable
        );
    }

    /// GraphQL requires authentication, so a deployment without `GITHUB_TOKEN`
    /// must never attempt it — and must keep working.
    #[test]
    fn the_graphql_fast_path_is_only_armed_when_a_token_exists() {
        assert!(!GitHubClient::with_token(None).unwrap().has_token);
        assert!(
            !GitHubClient::with_token(Some("   ".to_string()))
                .unwrap()
                .has_token,
            "a blank token is not a token"
        );
        assert!(GitHubClient::with_token(Some("ghp_example".to_string()))
            .unwrap()
            .has_token);
    }

    /// The live differential. Resolves the same refs through GraphQL and
    /// through REST against api.github.com and asserts they agree on the
    /// canonical URL, the ref name and — above all — the commit sha, which is
    /// both the report cache key and the archive download path.
    ///
    /// Ignored by default: it needs a token and the network.
    ///
    /// ```text
    /// GITHUB_TOKEN=$(gh auth token) cargo test github::tests::graphql_and_rest -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "hits api.github.com; needs GITHUB_TOKEN"]
    async fn graphql_and_rest_resolve_refs_identically() {
        let token = std::env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN must be set");
        let graphql = GitHubClient::with_token(Some(token.clone())).unwrap();
        let rest = GitHubClient::with_token(Some(token)).unwrap().without_graphql();

        let cases: &[(&str, Option<&str>)] = &[
            // default branch
            ("https://github.com/tokio-rs/axum", None),
            // lightweight tag
            ("https://github.com/vuejs/core", Some("v3.4.0")),
            // annotated tag: the case where a naive reading returns the tag
            // object's sha rather than the commit's
            ("https://github.com/torvalds/linux", Some("v6.6")),
            ("https://github.com/git/git", Some("v2.43.0")),
            // raw commit sha
            (
                "https://github.com/tokio-rs/axum",
                Some("a5116d6b1bcabdfd7039279e4957b4a9c0b50587"),
            ),
            // renamed repository, resolved under its old name
            ("https://github.com/vuejs/vue-next", None),
            // default branch that is not `main` — the case the service used to
            // fail on, and the one both paths must agree is `master`
            ("https://github.com/trinadhthatakula/Thor", None),
            // the same repository as a pasted browse URL: the ref comes out of
            // the path, so this must resolve identically to the line above
            ("https://github.com/trinadhthatakula/Thor/tree/master", None),
            // a ref *and* a subpath, the ambiguous shape: the first candidate
            // (`main/axum-core`) does not exist, the second (`main`) does
            ("https://github.com/tokio-rs/axum/tree/main/axum-core", None),
        ];

        for (url, ref_name) in cases {
            let from_graphql = graphql
                .resolve_ref(url, ref_name.map(str::to_string), true)
                .await
                .unwrap_or_else(|error| panic!("graphql failed for {url}@{ref_name:?}: {error}"));
            let from_rest = rest
                .resolve_ref(url, ref_name.map(str::to_string), true)
                .await
                .unwrap_or_else(|error| panic!("rest failed for {url}@{ref_name:?}: {error}"));

            println!(
                "{url}@{ref_name:?} -> {} ({})",
                from_graphql.commit_sha, from_graphql.ref_name
            );
            assert_eq!(
                from_graphql.commit_sha, from_rest.commit_sha,
                "commit sha diverged for {url}@{ref_name:?}"
            );
            assert_eq!(from_graphql.ref_name, from_rest.ref_name);
            assert_eq!(from_graphql.html_url, from_rest.html_url);
            assert_eq!(from_graphql.owner, from_rest.owner);
            assert_eq!(from_graphql.repo, from_rest.repo);
        }
    }

    /// The live half of the `main`-vs-`master` fix. Asking for `main` on a
    /// repository whose default branch is `master` must fail *and* say so —
    /// through both resolution paths, since a user only ever hits one of them.
    ///
    /// ```text
    /// GITHUB_TOKEN=$(gh auth token) cargo test github::tests::a_wrong_ref -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "hits api.github.com; needs GITHUB_TOKEN"]
    async fn a_wrong_ref_reports_the_real_default_branch_from_both_paths() {
        let token = std::env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN must be set");
        let graphql = GitHubClient::with_token(Some(token.clone())).unwrap();
        let rest = GitHubClient::with_token(Some(token)).unwrap().without_graphql();
        let url = "https://github.com/trinadhthatakula/Thor";

        for (label, client) in [("graphql", &graphql), ("rest", &rest)] {
            let error = client
                .resolve_ref(url, Some("main".to_string()), true)
                .await
                .expect_err("`main` does not exist in this repository");
            println!("{label}: {error:?}");
            // `GitHubError` cannot derive `PartialEq` — `Request` wraps a
            // `reqwest::Error` — so the shape is checked by hand.
            match error {
                GitHubError::RefNotFound { default_branch } => assert_eq!(
                    default_branch.as_deref(),
                    Some("master"),
                    "{label} lost the default branch"
                ),
                other => panic!("{label} reported {other:?}, not a missing ref"),
            }
        }

        // And the browse URL for that branch resolves without any ref argument.
        let resolved = graphql
            .resolve_ref(&format!("{url}/tree/master"), None, true)
            .await
            .unwrap();
        assert_eq!(resolved.ref_name, "master");
    }

    #[test]
    fn parses_https_urls() {
        let (owner, repo) =
            GitHubClient::parse_repo_owner_name("https://github.com/rust-lang/rust").unwrap();
        assert_eq!(owner, "rust-lang");
        assert_eq!(repo, "rust");
    }

    #[test]
    fn parses_git_urls() {
        let (owner, repo) =
            GitHubClient::parse_repo_owner_name("git@github.com:tokio-rs/axum.git").unwrap();
        assert_eq!(owner, "tokio-rs");
        assert_eq!(repo, "axum");
    }

    #[test]
    fn parses_gitlab_urls() {
        let (owner, repo) =
            GitHubClient::parse_repo_owner_name("https://gitlab.com/group/sub/project.git")
                .unwrap();
        assert_eq!(owner, "group/sub");
        assert_eq!(repo, "project");
    }

    #[test]
    fn rejects_unsupported_hosts() {
        assert!(GitHubClient::parse_repo_owner_name("https://example.com/a/b").is_err());
    }

    fn ref_candidates(input: &str) -> Vec<String> {
        GitHubClient::parse_repo_url(input)
            .unwrap()
            .url_ref_candidates
    }

    /// The regression that started this: pasting a browse URL used to throw the
    /// ref away, so `/tree/master` analysed whatever the default branch was.
    #[test]
    fn extracts_the_ref_from_a_browse_url() {
        assert_eq!(
            ref_candidates("https://github.com/trinadhthatakula/Thor/tree/master"),
            vec!["master".to_string()]
        );
        assert_eq!(
            ref_candidates("https://github.com/torvalds/linux/commit/ffc2532"),
            vec!["ffc2532".to_string()]
        );
    }

    /// `/tree/<ref>/<path>` cannot be disambiguated from a slashed ref by
    /// inspection, so both readings are offered, longest first.
    #[test]
    fn offers_both_readings_of_an_ambiguous_browse_path() {
        assert_eq!(
            ref_candidates("https://github.com/tokio-rs/axum/tree/main/axum-core"),
            vec!["main/axum-core".to_string(), "main".to_string()]
        );
        assert_eq!(
            ref_candidates("https://github.com/acme/app/blob/release/1.x/src/main.rs"),
            vec!["release/1.x/src/main.rs".to_string(), "release".to_string()]
        );
    }

    /// A plain repository URL, and a path shape this parser does not recognise,
    /// must contribute no candidates — resolution then uses the default branch
    /// exactly as it always did.
    #[test]
    fn contributes_no_ref_candidates_without_a_ref_marker() {
        assert!(ref_candidates("https://github.com/rust-lang/rust").is_empty());
        assert!(ref_candidates("https://github.com/rust-lang/rust/issues/42").is_empty());
        assert!(ref_candidates("https://github.com/rust-lang/rust/tree").is_empty());
        // GitLab refs sit behind `/-/`, which this parser folds into the project
        // path; guessing a ref out of it would be worse than not trying.
        assert!(ref_candidates("https://gitlab.com/group/sub/project").is_empty());
    }

    /// A ref reaches GitHub as data and gets encoded on the way out, so the
    /// escapes a browser put in the URL have to come off here. Left on,
    /// `release%2020.1` went out as `release%252020.1` and 404ed.
    #[test]
    fn decodes_percent_escapes_in_a_browse_ref() {
        assert_eq!(
            ref_candidates("https://github.com/acme/app/tree/release%2020.1"),
            vec!["release 20.1".to_string()]
        );
        // An encoded separator stays one path segment, so unlike a literal `/`
        // it is not ambiguous: the slash is part of the branch name and there is
        // only one reading to offer.
        assert_eq!(
            ref_candidates("https://github.com/acme/app/tree/release%2F1.x"),
            vec!["release/1.x".to_string()]
        );
        // `%` on its own is not an escape sequence, and `100%` is a legal branch
        // name: keep it rather than dropping the ref.
        assert_eq!(
            ref_candidates("https://github.com/acme/app/tree/100%"),
            vec!["100%".to_string()]
        );
    }

    /// Two distinct GitLab shapes used to fail the whole response on a bare
    /// `String` default branch, surfacing as an unreadable upstream body: a
    /// project with no commits, and a public project whose repository is
    /// member-only or disabled, where the field is withheld entirely because
    /// the caller lacks `read_code`. Only the first of those is empty, which is
    /// what `empty_repo` is read for.
    #[test]
    fn deserializes_a_gitlab_project_without_a_default_branch() {
        let body: GitLabProjectResponse = serde_json::from_str(
            r#"{
                "id": 42,
                "path_with_namespace": "group/empty",
                "default_branch": null,
                "web_url": "https://gitlab.com/group/empty",
                "visibility": "public",
                "empty_repo": true
            }"#,
        )
        .expect("a null default_branch must not fail the whole response");
        assert_eq!(body.default_branch, None);
        assert!(body.empty_repo);

        // Repository feature member-only or disabled: the field is missing
        // rather than null, and the project is not empty. The resolver has to
        // tell this apart from the case above — it answers `private_repo`, not
        // `empty_repository`.
        let body: GitLabProjectResponse = serde_json::from_str(
            r#"{
                "id": 44,
                "path_with_namespace": "group/issues-only",
                "web_url": "https://gitlab.com/group/issues-only",
                "visibility": "public"
            }"#,
        )
        .expect("a withheld default_branch must not fail the whole response");
        assert_eq!(body.default_branch, None);
        assert!(!body.empty_repo);

        // A populated project still deserializes, and `empty_repo` is absent
        // from older API responses.
        let body: GitLabProjectResponse = serde_json::from_str(
            r#"{
                "id": 43,
                "path_with_namespace": "group/app",
                "default_branch": "master",
                "web_url": "https://gitlab.com/group/app",
                "visibility": "public"
            }"#,
        )
        .expect("a populated project must still deserialize");
        assert_eq!(body.default_branch.as_deref(), Some("master"));
        assert!(!body.empty_repo);
    }
}
