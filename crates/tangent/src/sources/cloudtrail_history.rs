//! A cloudtrail source's past, as `tangent replay` reads it: the account's
//! CloudTrail Event history, every region's, through LookupEvents, in record
//! time order. Event history keeps the last 90 days of the management events
//! of the account the credentials are in, with no trail needed. Each
//! LookupEvents call is a management event itself, so a replay reads no
//! further than when it started, and leaves out any of its own calls it
//! reads back anyway.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use aws_sdk_cloudtrail::config::retry::RetryConfig;
use aws_sdk_cloudtrail::config::{AppName, Region as AwsRegion};
use aws_sdk_cloudtrail::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_cloudtrail::primitives::DateTime as AwsTime;
use aws_sdk_cloudtrail::Client;
use chrono::{DateTime, SubsecRound, TimeDelta, Utc};
use serde_json::Value;
use tokio::time::Instant;

use crate::sources::poll::parse_time;

/// AWS's commercial (`aws` partition) regions: those every account has, then
/// the opt-in ones an account enables. From botocore's
/// `botocore/data/endpoints.json` (2026-10-30), split as AWS Account
/// Management's region reference splits them. A region AWS adds needs a line.
pub const DEFAULT_REGIONS: [&str; 17] = [
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "ca-central-1",
    "eu-central-1",
    "eu-north-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
];
pub const OPT_IN_REGIONS: [&str; 17] = [
    "af-south-1",
    "ap-east-1",
    "ap-east-2",
    "ap-south-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-6",
    "ap-southeast-7",
    "ca-west-1",
    "eu-central-2",
    "eu-south-1",
    "eu-south-2",
    "il-central-1",
    "me-central-1",
    "me-south-1",
    "mx-central-1",
];
pub const ALL_REGIONS: usize = DEFAULT_REGIONS.len() + OPT_IN_REGIONS.len();

/// How far back Event history goes.
pub fn kept() -> TimeDelta {
    TimeDelta::days(90)
}

/// How long after a call CloudTrail typically has its event: within about
/// five minutes, not guaranteed.
pub fn lag() -> TimeDelta {
    TimeDelta::minutes(5)
}

/// LookupEvents takes two calls a second per account and region.
const PACE: Duration = Duration::from_millis(500);

/// The app name a replay's calls carry in their user agent: `tangent-replay`
/// and an id of the replay's own, so it can tell its calls from an earlier
/// replay's.
pub fn app_name() -> String {
    format!("tangent-replay-{:08x}", rand::random::<u32>())
}

/// Where a replay started at `started` stops reading `[from, to)`: at `to`,
/// or at `started` if that is sooner, since Event history after it holds the
/// replay's own calls. `started` goes to the second, as `eventTime` does, so
/// a call in the second the replay started is left out too. A `from` before
/// what Event history keeps is refused.
pub fn range_end(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    started: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    let started = started.trunc_subsecs(0);
    ensure!(
        from >= started - kept(),
        "from ({from}) is more than 90 days ago: Event history keeps the last 90 days"
    );
    let end = to.min(started);
    ensure!(
        from < end,
        "from ({from}) is not before the replay started ({started}): Event history has nothing to read"
    );
    Ok(end)
}

/// A page of a region's Event history.
pub struct Page {
    /// Each event's `CloudTrailEvent`: the record, as a trail file holds it.
    pub events: Vec<String>,
    pub next: Option<String>,
}

/// What LookupEvents in a region answers.
pub enum Answer {
    Page(Page),
    /// An opt-in region the account hasn't enabled.
    NotEnabled,
    /// An opt-in region whose endpoint could not be reached on its first
    /// call: what failed.
    Unreachable(String),
}

/// LookupEvents in a region, so the history can be read without AWS.
#[async_trait]
pub trait Lookup: Send + Sync {
    /// The events from `start` to `end`, both included, newest first, a
    /// page at a time.
    async fn lookup(
        &self,
        region: &'static str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        token: Option<String>,
    ) -> Result<Answer>;
}

/// A CloudTrail client in each region, as the source's credentials, named
/// `app` in its user agent; and the opt-in regions asked once already.
pub struct Clients(
    Vec<(&'static str, Client)>,
    parking_lot::Mutex<HashSet<&'static str>>,
);

impl Clients {
    pub fn new(aws: &aws_config::SdkConfig, app: &str) -> Result<Self> {
        let app = AppName::new(app.to_string()).map_err(|e| anyhow!("app name {app}: {e}"))?;
        Ok(Self(
            DEFAULT_REGIONS
                .into_iter()
                .chain(OPT_IN_REGIONS)
                .map(|r| {
                    let conf = aws_sdk_cloudtrail::config::Builder::from(aws)
                        .region(AwsRegion::new(r))
                        .app_name(app.clone())
                        // the region's two a second are shared with every caller: when
                        // throttled, back off for longer than the default three tries
                        .retry_config(RetryConfig::standard().with_max_attempts(10))
                        .build();
                    (r, Client::from_conf(conf))
                })
                .collect(),
            Default::default(),
        ))
    }
}

#[async_trait]
impl Lookup for Clients {
    async fn lookup(
        &self,
        region: &'static str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        token: Option<String>,
    ) -> Result<Answer> {
        let (_, client) = self
            .0
            .iter()
            .find(|(r, _)| *r == region)
            .context("no client for the region")?;
        let call = || {
            client
                .lookup_events()
                .start_time(AwsTime::from_millis(start.timestamp_millis()))
                .end_time(AwsTime::from_millis(end.timestamp_millis()))
                .max_results(50)
                .set_next_token(token.clone())
        };
        // An opt-in region's first call is not retried: a disabled region
        // answers at once, but one whose endpoint can't be reached would
        // spend every retry on connecting. Throttled, it is enabled: ask
        // again with retries.
        let first = OPT_IN_REGIONS.contains(&region) && self.1.lock().insert(region);
        let mut res = if first {
            call()
                .customize()
                .config_override(
                    aws_sdk_cloudtrail::config::Builder::default()
                        .retry_config(RetryConfig::disabled()),
                )
                .send()
                .await
        } else {
            call().send().await
        };
        if first && res.as_ref().err().and_then(|e| e.code()) == Some("ThrottlingException") {
            res = call().send().await;
        }
        match res {
            Ok(out) => Ok(Answer::Page(Page {
                events: out
                    .events
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|e| e.cloud_trail_event)
                    .collect(),
                next: out.next_token,
            })),
            // a disabled opt-in region holds none of the account's credentials
            Err(e)
                if OPT_IN_REGIONS.contains(&region)
                    && e.code() == Some("UnrecognizedClientException") =>
            {
                Ok(Answer::NotEnabled)
            }
            Err(e @ (SdkError::DispatchFailure(_) | SdkError::TimeoutError(_))) if first => {
                Ok(Answer::Unreachable(DisplayErrorContext(&e).to_string()))
            }
            Err(e) => Err(anyhow!("{}", DisplayErrorContext(&e))),
        }
    }
}

struct Region {
    name: &'static str,
    next_call: Instant,
}

/// Event history's records in `[from, end)`, read at most an hour at a time
/// in every region and handed out in time order.
pub struct History {
    lookup: Box<dyn Lookup>,
    /// The app name the replay's calls carry in their `userAgent`.
    app: String,
    regions: Vec<Region>,
    from: DateTime<Utc>,
    end: DateTime<Utc>,
    next: DateTime<Utc>,
    pace: Duration,
    /// Regions the account hasn't enabled.
    pub not_enabled: usize,
    /// Opt-in regions whose endpoint could not be reached.
    pub unreachable: usize,
    pub calls: u64,
    pub records: u64,
    /// The replay's own LookupEvents calls read back and left out.
    pub own: u64,
}

impl History {
    /// The history `[from, end)` through `lookup`, whose calls carry `app`
    /// in their user agent.
    pub fn new(
        lookup: Box<dyn Lookup>,
        from: DateTime<Utc>,
        end: DateTime<Utc>,
        app: String,
    ) -> Self {
        let now = Instant::now();
        Self {
            lookup,
            app,
            regions: DEFAULT_REGIONS
                .into_iter()
                .chain(OPT_IN_REGIONS)
                .map(|name| Region {
                    name,
                    next_call: now,
                })
                .collect(),
            from,
            end,
            next: from,
            pace: PACE,
            not_enabled: 0,
            unreachable: 0,
            calls: 0,
            records: 0,
            own: 0,
        }
    }

    /// The regions read: every one but those left out.
    pub fn regions(&self) -> usize {
        self.regions.len()
    }

    /// Where the range ends.
    pub fn end(&self) -> DateTime<Utc> {
        self.end
    }

    /// The range's records, going by those read so far: as many to an hour.
    /// None before any is read.
    pub fn estimate(&self) -> Option<u64> {
        let read = (self.next - self.from).num_seconds();
        let all = (self.end - self.from).num_seconds();
        (read > 0).then(|| (u128::from(self.records) * all as u128 / read as u128) as u64)
    }

    /// Every record before `until` not yet handed out, in time order, with
    /// its time.
    pub async fn until(&mut self, until: DateTime<Utc>) -> Result<Vec<(DateTime<Utc>, Value)>> {
        let until = until.min(self.end);
        let mut out = Vec::new();
        while self.next < until {
            let end = (self.next + TimeDelta::hours(1)).min(until);
            out.extend(self.slice(self.next, end).await?);
            self.next = end;
        }
        Ok(out)
    }

    /// Every region's records in `[start, end)`, in time order, ties by
    /// `eventID`. One without an `eventTime` takes `start`. The replay's own
    /// calls are left out: the range ends when it started, but a clock ahead
    /// of AWS's puts its first calls before that.
    async fn slice(
        &mut self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<(DateTime<Utc>, Value)>> {
        let (lookup, pace) = (self.lookup.as_ref(), self.pace);
        let read = futures::future::try_join_all(
            self.regions
                .iter_mut()
                .map(|r| region(lookup, r, pace, start, end)),
        )
        .await?;
        let (mut events, mut kept) = (Vec::new(), Vec::new());
        for (r, (got, calls)) in std::mem::take(&mut self.regions).into_iter().zip(read) {
            self.calls += calls;
            match got {
                Answer::Page(page) => {
                    events.extend(page.events);
                    kept.push(r);
                }
                Answer::NotEnabled => {
                    tracing::debug!(
                        region = r.name,
                        "not enabled in the account: no Event history to read"
                    );
                    self.not_enabled += 1;
                }
                Answer::Unreachable(msg) => {
                    tracing::warn!(
                        region = r.name,
                        "could not reach LookupEvents in this opt-in region, so its Event history (if the account has enabled it) is left out: {msg}"
                    );
                    self.unreachable += 1;
                }
            }
        }
        self.regions = kept;

        let mut out = Vec::new();
        for raw in events {
            let v: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("an event whose CloudTrailEvent is not JSON, skipped: {e}");
                    continue;
                }
            };
            let t = parse_time(&v["eventTime"]).unwrap_or(start);
            if t < start || t >= end {
                continue;
            }
            if v["userAgent"]
                .as_str()
                .is_some_and(|ua| ua.contains(&self.app))
            {
                self.own += 1;
                continue;
            }
            out.push((t, v));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| id(&a.1).cmp(&id(&b.1))));
        self.records += out.len() as u64;
        Ok(out)
    }
}

fn id(v: &Value) -> Option<&str> {
    v.get("eventID").and_then(Value::as_str)
}

/// A region's events from `start` to `end`, every page's as one, its calls
/// `pace` apart, and how many calls it took. A NextToken that is empty ends
/// the pages, as one that is missing does; one that comes back as sent is
/// an error, not a page to ask for again.
async fn region(
    lookup: &dyn Lookup,
    r: &mut Region,
    pace: Duration,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(Answer, u64)> {
    let (mut events, mut token, mut calls) = (Vec::new(), None::<String>, 0);
    loop {
        tokio::time::sleep_until(r.next_call).await;
        r.next_call = Instant::now() + pace;
        calls += 1;
        let page = match lookup
            .lookup(r.name, start, end, token.clone())
            .await
            .with_context(|| format!("LookupEvents in {}", r.name))?
        {
            Answer::Page(page) => page,
            skip => return Ok((skip, calls)),
        };
        events.extend(page.events);
        match page.next.filter(|next| !next.is_empty()) {
            Some(next) if token.as_ref() == Some(&next) => {
                bail!("LookupEvents in {}: NextToken repeated", r.name)
            }
            Some(next) => token = Some(next),
            None => return Ok((Answer::Page(Page { events, next: None }), calls)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::parse_when;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    type Asked = Arc<Mutex<Vec<(&'static str, DateTime<Utc>, Instant)>>>;

    const APP: &str = "tangent-replay-0000000a";

    /// Event history in memory: each region's events, newest first in pages of
    /// `page` (50 when 0), as LookupEvents answers; `disabled` regions answer
    /// not enabled, `failing` ones an error. Each call's region, end and moment
    /// go in `asked`.
    #[derive(Default)]
    struct Mem {
        events: HashMap<&'static str, Vec<Value>>,
        disabled: Vec<&'static str>,
        unreachable: Vec<&'static str>,
        failing: Vec<&'static str>,
        page: usize,
        asked: Asked,
    }

    #[async_trait]
    impl Lookup for Mem {
        async fn lookup(
            &self,
            region: &'static str,
            start: DateTime<Utc>,
            end: DateTime<Utc>,
            token: Option<String>,
        ) -> Result<Answer> {
            self.asked
                .lock()
                .unwrap()
                .push((region, end, Instant::now()));
            if self.disabled.contains(&region) {
                return Ok(Answer::NotEnabled);
            }
            if self.unreachable.contains(&region) {
                return Ok(Answer::Unreachable("connect timeout".into()));
            }
            ensure!(!self.failing.contains(&region), "ThrottlingException");
            let mut hits: Vec<&Value> = self
                .events
                .get(region)
                .into_iter()
                .flatten()
                .filter(|v| parse_time(&v["eventTime"]).is_none_or(|t| t >= start && t <= end))
                .collect();
            hits.sort_by_key(|v| std::cmp::Reverse(parse_time(&v["eventTime"])));
            let at: usize = token.map_or(0, |t| t.parse().unwrap());
            let page = if self.page == 0 { 50 } else { self.page };
            Ok(Answer::Page(Page {
                events: hits
                    .iter()
                    .skip(at)
                    .take(page)
                    .map(|v| v.to_string())
                    .collect(),
                next: (at + page < hits.len()).then(|| (at + page).to_string()),
            }))
        }
    }

    fn at(s: &str) -> DateTime<Utc> {
        parse_when(s).unwrap()
    }

    fn event(time: &str, id: &str) -> Value {
        json!({"eventTime": time, "eventID": id, "eventName": "DescribeInstances"})
    }

    fn history(lookup: impl Lookup + 'static, from: &str, end: &str) -> History {
        let mut h = History::new(Box::new(lookup), at(from), at(end), APP.into());
        h.pace = Duration::ZERO;
        h
    }

    fn ids(got: &[(DateTime<Utc>, Value)]) -> Vec<&str> {
        got.iter()
            .map(|(_, v)| v["eventID"].as_str().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn every_region_in_time_order() {
        let mem = Mem {
            events: HashMap::from([
                (
                    "us-east-1",
                    vec![
                        event("2026-10-01T09:59:59Z", "before"),
                        event("2026-10-01T10:05:00Z", "a1"),
                        event("2026-10-01T10:20:00Z", "b"),
                        event("2026-10-01T10:40:00Z", "a3"),
                    ],
                ),
                (
                    "eu-west-1",
                    vec![
                        event("2026-10-01T10:20:00Z", "a"),
                        event("2026-10-01T10:30:00Z", "e1"),
                        // on the boundary: both hours' calls return it, the second's keeps it
                        event("2026-10-01T11:00:00Z", "edge"),
                    ],
                ),
                ("ap-southeast-2", vec![event("2026-10-01T11:30:00Z", "p1")]),
            ]),
            ..Mem::default()
        };
        let mut h = history(mem, "2026-10-01T10:00:00Z", "2026-10-01T12:00:00Z");
        let first = h.until(at("2026-10-01T11:00:00Z")).await.unwrap();
        assert_eq!(ids(&first), ["a1", "a", "b", "e1", "a3"]);
        let second = h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
        assert_eq!(ids(&second), ["edge", "p1"]);
        assert!(h
            .until(at("2026-10-01T13:00:00Z"))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(h.records, 7);
        assert_eq!(h.calls, 2 * 34, "a call a region an hour");
    }

    #[tokio::test]
    async fn a_regions_pages_in_turn_paced() {
        let asked = Asked::default();
        let mem = Mem {
            events: HashMap::from([(
                "us-west-2",
                (0..5)
                    .map(|m| event(&format!("2026-10-01T10:0{m}:00Z"), &format!("w{m}")))
                    .collect(),
            )]),
            page: 2,
            asked: Arc::clone(&asked),
            ..Mem::default()
        };
        let mut h = history(mem, "2026-10-01T10:00:00Z", "2026-10-01T11:00:00Z");
        h.pace = Duration::from_millis(100);
        let got = h.until(at("2026-10-01T11:00:00Z")).await.unwrap();
        assert_eq!(ids(&got), ["w0", "w1", "w2", "w3", "w4"]);
        assert_eq!(h.calls, 3 + 33);
        let calls: Vec<Instant> = asked
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.0 == "us-west-2")
            .map(|c| c.2)
            .collect();
        assert_eq!(calls.len(), 3);
        assert!(
            calls
                .windows(2)
                .all(|w| w[1] - w[0] >= Duration::from_millis(100)),
            "a region's calls are paced"
        );
    }

    /// A region whose NextToken comes back unchanged, or as "" on its last
    /// page: either would have the pages go on forever, each call a
    /// management event, as the SDK's own paginator guards against.
    struct Tokens {
        repeat: bool,
    }

    #[async_trait]
    impl Lookup for Tokens {
        async fn lookup(
            &self,
            region: &'static str,
            _start: DateTime<Utc>,
            _end: DateTime<Utc>,
            token: Option<String>,
        ) -> Result<Answer> {
            let page = |id: &str, next: &str| {
                Answer::Page(Page {
                    events: vec![
                        json!({"eventTime": "2026-10-01T10:05:00Z", "eventID": id}).to_string()
                    ],
                    next: Some(next.into()),
                })
            };
            Ok(match (region, token.as_deref()) {
                ("us-east-1", _) if self.repeat => page("a", "same"),
                // a service that reads "" as no token would start over
                ("us-east-1", None | Some("")) => page("a", "2"),
                ("us-east-1", Some(_)) => page("b", ""),
                _ => Answer::Page(Page {
                    events: Vec::new(),
                    next: None,
                }),
            })
        }
    }

    #[tokio::test]
    async fn an_empty_token_ends_the_pages_a_repeated_one_fails() {
        let (start, end) = ("2026-10-01T10:00:00Z", "2026-10-01T11:00:00Z");
        let mut h = history(Tokens { repeat: false }, start, end);
        let got = h.until(at(end)).await.unwrap();
        assert_eq!(ids(&got), ["a", "b"]);
        assert_eq!(h.calls, 2 + 33);

        let mut h = history(Tokens { repeat: true }, start, end);
        let e = h.until(at(end)).await.unwrap_err();
        assert!(
            format!("{e:#}").contains("LookupEvents in us-east-1: NextToken repeated"),
            "{e:#}"
        );
    }

    #[tokio::test]
    async fn a_region_not_enabled_is_skipped_another_error_fails() {
        let mem = Mem {
            events: HashMap::from([("us-east-1", vec![event("2026-10-01T10:05:00Z", "a")])]),
            disabled: vec!["af-south-1", "me-south-1"],
            ..Mem::default()
        };
        let mut h = history(mem, "2026-10-01T10:00:00Z", "2026-10-01T12:00:00Z");
        assert_eq!(
            ids(&h.until(at("2026-10-01T11:00:00Z")).await.unwrap()),
            ["a"]
        );
        assert_eq!((h.regions(), h.not_enabled), (32, 2));
        h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
        assert_eq!(h.calls, 34 + 32, "a region skipped is not asked again");

        let mem = Mem {
            failing: vec!["eu-central-1"],
            ..Mem::default()
        };
        let mut h = history(mem, "2026-10-01T10:00:00Z", "2026-10-01T11:00:00Z");
        let e = h.until(at("2026-10-01T11:00:00Z")).await.unwrap_err();
        assert!(
            format!("{e:#}").contains("LookupEvents in eu-central-1"),
            "{e:#}"
        );
    }

    #[tokio::test]
    async fn an_opt_in_region_out_of_reach_is_skipped() {
        let mem = Mem {
            events: HashMap::from([("us-east-1", vec![event("2026-10-01T10:05:00Z", "a")])]),
            unreachable: vec!["me-south-1"],
            ..Mem::default()
        };
        let mut h = history(mem, "2026-10-01T10:00:00Z", "2026-10-01T12:00:00Z");
        assert_eq!(
            ids(&h.until(at("2026-10-01T11:00:00Z")).await.unwrap()),
            ["a"]
        );
        assert_eq!((h.regions(), h.unreachable), (33, 1));
        h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
        assert_eq!(h.calls, 34 + 33, "a region out of reach is not asked again");
    }

    /// An opt-in region whose endpoint refuses connections answers on its
    /// first call, without the retries that would spend a minute on it.
    #[tokio::test]
    async fn clients_skip_an_opt_in_region_they_cannot_connect_to_at_once() {
        // a port nothing listens on
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(AwsRegion::new("us-east-1"))
            .credentials_provider(aws_sdk_cloudtrail::config::Credentials::new(
                "AKID", "secret", None, None, "test",
            ))
            .endpoint_url(format!("http://{addr}"))
            .load()
            .await;
        let clients = Clients::new(&aws, APP).unwrap();
        let (start, end) = (at("2026-10-01T10:00:00Z"), at("2026-10-01T11:00:00Z"));
        let asked = std::time::Instant::now();
        assert!(matches!(
            clients
                .lookup("me-south-1", start, end, None)
                .await
                .unwrap(),
            Answer::Unreachable(_)
        ));
        assert!(
            asked.elapsed() < Duration::from_secs(2),
            "{:?}",
            asked.elapsed()
        );
    }

    #[tokio::test]
    async fn the_range_stops_when_the_replay_started() {
        let started = at("2026-10-01T11:30:00.250Z");
        let end = range_end(
            at("2026-10-01T10:00:00Z"),
            at("2026-10-01T13:00:00Z"),
            started,
        )
        .unwrap();
        assert_eq!(end, at("2026-10-01T11:30:00Z"), "to the second");
        let asked = Asked::default();
        let mem = Mem {
            events: HashMap::from([(
                "us-east-1",
                vec![
                    event("2026-10-01T11:29:59Z", "last"),
                    // a call in the second the replay started
                    event("2026-10-01T11:30:00Z", "own"),
                ],
            )]),
            asked: Arc::clone(&asked),
            ..Mem::default()
        };
        let mut h = History::new(Box::new(mem), at("2026-10-01T10:00:00Z"), end, APP.into());
        h.pace = Duration::ZERO;
        let mut got = Vec::new();
        for hour in ["11:00", "12:00", "13:00"] {
            got.extend(
                h.until(at(&format!("2026-10-01T{hour}:00Z")))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(ids(&got), ["last"]);
        assert!(asked.lock().unwrap().iter().all(|c| c.1 <= started));
        assert_eq!(h.estimate(), Some(1));

        assert_eq!(
            range_end(
                at("2026-10-01T10:00:00Z"),
                at("2026-10-01T11:00:00Z"),
                started
            )
            .unwrap(),
            at("2026-10-01T11:00:00Z"),
            "a range before the start is whole"
        );
        assert!(range_end(
            at("2026-10-01T12:00:00Z"),
            at("2026-10-01T13:00:00Z"),
            started
        )
        .is_err());
    }

    /// Event history whose clock runs 3 s behind the replay's: each call
    /// lands in its region's history as a LookupEvents event at AWS's time,
    /// once answered, and the clock moves 10 ms a call. An earlier replay's
    /// call is there already.
    struct Skewed {
        aws_now: Mutex<DateTime<Utc>>,
        logged: Mutex<Vec<(&'static str, Value)>>,
    }

    fn lookup_event(time: DateTime<Utc>, id: &str, app: &str) -> Value {
        json!({
            "eventTime": time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "eventID": id,
            "eventSource": "cloudtrail.amazonaws.com",
            "eventName": "LookupEvents",
            "userAgent": format!("aws-sdk-rust/1.3.10 ua/2.1 api/cloudtrail#1.126.0 os/macos lang/rust#1.91.0 app/{app}"),
        })
    }

    #[async_trait]
    impl Lookup for Skewed {
        async fn lookup(
            &self,
            region: &'static str,
            start: DateTime<Utc>,
            end: DateTime<Utc>,
            _token: Option<String>,
        ) -> Result<Answer> {
            let mut logged = self.logged.lock().unwrap();
            let events = logged
                .iter()
                .filter(|(r, v)| {
                    let t = parse_time(&v["eventTime"]).unwrap();
                    *r == region && t >= start && t <= end
                })
                .map(|(_, v)| v.to_string())
                .collect();
            let mut now = self.aws_now.lock().unwrap();
            let n = logged.len();
            logged.push((region, lookup_event(*now, &format!("own-{n}"), APP)));
            *now += TimeDelta::milliseconds(10);
            Ok(Answer::Page(Page { events, next: None }))
        }
    }

    /// A clock ahead of AWS's puts the replay's first calls before the
    /// range's end; the last hour reads them back and leaves them out by
    /// their user agent, but keeps an earlier replay's.
    #[tokio::test]
    async fn a_clock_ahead_of_aws_reads_no_call_of_its_own() {
        let started = at("2026-10-01T12:00:00Z");
        let from = started - TimeDelta::hours(2);
        let end = range_end(from, started, started).unwrap();
        let earlier = lookup_event(
            at("2026-10-01T11:30:00Z"),
            "earlier",
            "tangent-replay-0000000b",
        );
        let mem = Skewed {
            aws_now: Mutex::new(started - TimeDelta::seconds(3)),
            logged: Mutex::new(vec![("us-east-1", earlier)]),
        };
        let mut h = History::new(Box::new(mem), from, end, APP.into());
        h.pace = Duration::ZERO;
        let mut got = h.until(from + TimeDelta::hours(1)).await.unwrap();
        got.extend(h.until(end).await.unwrap());
        assert_eq!(ids(&got), ["earlier"]);
        assert_eq!(h.own, 34, "the first hour's calls, one a region");
    }

    #[test]
    fn ninety_days_back_at_most() {
        let started = at("2026-11-04T12:00:00Z");
        assert!(range_end(started - TimeDelta::days(90), started, started).is_ok());
        let e = range_end(started - TimeDelta::days(91), started, started).unwrap_err();
        assert!(
            e.to_string()
                .contains("Event history keeps the last 90 days"),
            "{e}"
        );
    }

    #[test]
    fn each_replay_names_itself() {
        let (a, b) = (app_name(), app_name());
        assert_ne!(a, b);
        assert!(
            a.starts_with("tangent-replay-") && a.len() == b.len(),
            "{a}"
        );
        assert!(AppName::new(a).is_ok());
    }

    #[tokio::test]
    async fn the_estimate_scales_the_first_hour_to_the_range() {
        let mem = Mem {
            events: HashMap::from([(
                "us-east-1",
                (0..4)
                    .map(|m| event(&format!("2026-10-01T10:1{m}:00Z"), &format!("e{m}")))
                    .collect(),
            )]),
            ..Mem::default()
        };
        let mut h = history(mem, "2026-10-01T10:00:00Z", "2026-10-01T13:00:00Z");
        assert_eq!(h.estimate(), None);
        h.until(at("2026-10-01T11:00:00Z")).await.unwrap();
        assert_eq!(h.estimate(), Some(12));
    }

    #[tokio::test]
    async fn a_record_without_a_time_takes_its_hours_start() {
        let mem = Mem {
            events: HashMap::from([(
                "us-east-1",
                vec![json!({"eventID": "x", "eventName": "ConsoleLogin"})],
            )]),
            ..Mem::default()
        };
        let mut h = history(mem, "2026-10-01T10:00:00Z", "2026-10-01T11:00:00Z");
        let got = h.until(at("2026-10-01T11:00:00Z")).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, at("2026-10-01T10:00:00Z"));
    }

    /// The clients against a stand-in for CloudTrail that refuses every call as
    /// a disabled region does: an opt-in region is skipped, and a default one
    /// (which can't be disabled) fails. Each call names the replay in its user
    /// agent.
    #[tokio::test]
    async fn clients_skip_a_disabled_opt_in_region() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let named = Arc::new(Mutex::new(Vec::<bool>::new()));
        let seen = Arc::clone(&named);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 16 * 1024];
                let n = sock.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]).to_lowercase();
                seen.lock()
                    .unwrap()
                    .push(req.contains(&format!("app/{APP}")));
                let body = r#"{"__type":"UnrecognizedClientException","message":"The security token included in the request is invalid."}"#;
                let resp = format!("HTTP/1.1 403 Forbidden\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                sock.write_all(resp.as_bytes()).await.unwrap();
            }
        });
        let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(AwsRegion::new("us-east-1"))
            .credentials_provider(aws_sdk_cloudtrail::config::Credentials::new(
                "AKID", "secret", None, None, "test",
            ))
            .endpoint_url(format!("http://{addr}"))
            .load()
            .await;
        let clients = Clients::new(&aws, APP).unwrap();
        let (start, end) = (at("2026-10-01T10:00:00Z"), at("2026-10-01T11:00:00Z"));
        assert!(matches!(
            clients
                .lookup("af-south-1", start, end, None)
                .await
                .unwrap(),
            Answer::NotEnabled
        ));
        let e = clients
            .lookup("us-east-1", start, end, None)
            .await
            .err()
            .unwrap();
        assert!(e.to_string().contains("UnrecognizedClientException"), "{e}");
        assert_eq!(*named.lock().unwrap(), [true, true]);
    }
}
