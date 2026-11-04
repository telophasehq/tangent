//! A cloudtrail source's past, as `tangent replay` reads it: the account's
//! CloudTrail Event history, every region's, through LookupEvents, in record
//! time order. Event history keeps the last 90 days of the management events
//! of the account the credentials are in, with no trail needed. Each
//! LookupEvents call is a management event itself, so a replay reads no
//! further than when it started, and leaves out any of its own calls it
//! reads back anyway, and earlier replays' from this machine, as its ledger
//! of replays records them.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use aws_sdk_cloudtrail::config::retry::RetryConfig;
use aws_sdk_cloudtrail::config::{
    AppName, ConfigBag, Credentials, IdentityCache, Region as AwsRegion, ResolveCachedIdentity,
    RuntimeComponents, SharedIdentityCache,
};
use aws_sdk_cloudtrail::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_cloudtrail::primitives::DateTime as AwsTime;
use aws_sdk_cloudtrail::Client;
use aws_smithy_runtime_api::client::identity::{IdentityFuture, SharedIdentityResolver};
use chrono::{DateTime, SecondsFormat, SubsecRound, TimeDelta, Utc};
use serde_json::{json, Value};
use tangent_shared::sources::cloudtrail::CloudtrailConfig;
use tokio::time::Instant;

use crate::replay::Past;
use crate::sources::cloudtrail::{aws, SOURCE};
use crate::sources::poll::{parse_time, stamp};

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

/// How far apart this machine's clock and AWS's may be: a replay's call can
/// land in Event history this much before it started or after it finished.
fn skew() -> TimeDelta {
    TimeDelta::minutes(1)
}

/// The longest an STS session lasts (GetSessionToken's and
/// GetFederationToken's 36 hours): a replay killed before its finish line
/// counts as running no longer, whatever its keys' expiry says.
fn longest_session() -> TimeDelta {
    TimeDelta::hours(36)
}

/// Where this machine's replays are recorded:
/// `$XDG_STATE_HOME/tangent/replays.jsonl`, else
/// `~/.local/state/tangent/replays.jsonl`. None, with a warning, when
/// neither is set.
pub fn ledger_path() -> Option<PathBuf> {
    let var = |name| {
        std::env::var_os(name)
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
    };
    let state = var("XDG_STATE_HOME").or_else(|| Some(var("HOME")?.join(".local/state")));
    if state.is_none() {
        tracing::warn!("neither XDG_STATE_HOME nor HOME is set, so there is no ledger of this machine's replays: earlier replays' LookupEvents calls stay in, and a later replay will read this one's");
    }
    Some(state?.join("tangent/replays.jsonl"))
}

/// A replay, as the machine's ledger of replays records it: a random 64-bit
/// id, which its calls carry in their user agent as `tangent-replay-<id>`;
/// when it started; and the access keys its calls are signed with, as the
/// SDK resolves them. Its first signed call writes its start line, each new
/// key after that a line of its own, and its History its finish, once it
/// makes no more calls: its range read, or the replay failed or stopped.
#[derive(Debug)]
pub struct Run {
    id: String,
    started: DateTime<Utc>,
    ledger: Option<PathBuf>,
    keys: parking_lot::Mutex<Vec<String>>,
    finished: AtomicBool,
    warned: AtomicBool,
}

impl Run {
    /// A replay started at `started`, recorded in the ledger at `ledger`.
    pub fn new(started: DateTime<Utc>, ledger: Option<PathBuf>) -> Self {
        Self {
            id: format!("{:016x}", rand::random::<u64>()),
            started,
            ledger,
            keys: Default::default(),
            finished: AtomicBool::new(false),
            warned: AtomicBool::new(false),
        }
    }

    /// The app name its calls carry in their user agent.
    pub fn app(&self) -> String {
        format!("tangent-replay-{}", self.id)
    }

    /// Its calls are signed with `key`, which expires at `expires` if it is
    /// temporary: a key not seen yet goes in the ledger, the first with the
    /// start.
    fn signs_with(&self, key: &str, expires: Option<SystemTime>) {
        let mut keys = self.keys.lock();
        if keys.iter().any(|k| k == key) {
            return;
        }
        let expires = expires.map(|t| rfc3339(t.into()));
        let line = if keys.is_empty() {
            json!({"id": self.id, "access_key_id": key, "started": rfc3339(self.started), "key_expires": expires})
        } else {
            json!({"id": self.id, "access_key_id": key, "key_expires": expires})
        };
        keys.push(key.to_string());
        self.append(&line);
    }

    /// It finished at `at`: a line in the ledger, once it has a start line,
    /// and only the first time.
    fn finished(&self, at: DateTime<Utc>) {
        if !self.keys.lock().is_empty() && !self.finished.swap(true, Ordering::Relaxed) {
            self.append(&json!({"id": self.id, "finished": rfc3339(at)}));
        }
    }

    /// Its calls so far, up to `now`.
    fn calls(&self, now: DateTime<Utc>) -> Calls {
        Calls {
            tag: tag(&self.id),
            keys: self.keys.lock().clone(),
            from: self.started - skew(),
            until: now,
            left_out: 0,
        }
    }

    /// The calls of the earlier replays the ledger records that could be
    /// in `[from, end)`.
    fn earlier(&self, from: DateTime<Utc>, end: DateTime<Utc>) -> Vec<Calls> {
        let Some(path) = &self.ledger else {
            return Vec::new();
        };
        let (runs, bad) = match read_ledger(path) {
            Ok(read) => read,
            Err(e) => {
                self.warn(format_args!("could not use the ledger of this machine's replays at {}, so earlier replays' LookupEvents calls stay in: {e}", path.display()));
                return Vec::new();
            }
        };
        // apart from the warning above, so a line left torn for good can't
        // hide a ledger this replay can't write
        if bad > 0 {
            tracing::warn!(
                "lines of the ledger of this machine's replays at {} that are not a replay's are skipped ({bad})",
                path.display()
            );
        }
        let tag = tag(&self.id);
        runs.into_iter()
            .filter(|r| r.tag != tag && r.until >= from && r.from < end)
            .collect()
    }

    fn append(&self, line: &Value) {
        let Some(path) = &self.ledger else {
            return;
        };
        if let Err(e) = append(path, line) {
            self.warn(format_args!("could not write the ledger of this machine's replays at {}, so a later replay will read this one's LookupEvents calls: {e}", path.display()));
        }
    }

    /// A ledger it can't read or write, said once: the replay goes on.
    fn warn(&self, msg: std::fmt::Arguments) {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{msg}; the replay goes on");
        }
    }
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// What the user agent of the replay `id`'s calls holds.
fn tag(id: &str) -> String {
    format!("app/tangent-replay-{id}")
}

/// `line` at the end of the ledger at `path`, in one write with O_APPEND,
/// so replays writing at once don't interleave. A last line left torn (by a
/// crash or a full disk) is ended first, so it doesn't take this one with
/// it. The ledger's directory is made 0700 and the file 0600.
fn append(path: &Path, line: &Value) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        let mut d = std::fs::DirBuilder::new();
        d.recursive(true);
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut d, 0o700);
        d.create(dir)?;
    }
    let mut f = std::fs::OpenOptions::new();
    f.read(true).append(true).create(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut f, 0o600);
    let mut f = f.open(path)?;
    let mut last = *b"\n";
    if let Some(at) = f.metadata()?.len().checked_sub(1) {
        f.seek(SeekFrom::Start(at))?;
        f.read_exact(&mut last)?;
    }
    let torn = if last == *b"\n" { "" } else { "\n" };
    f.write_all(format!("{torn}{line}\n").as_bytes())
}

/// The calls of every replay the ledger at `path` records (none if there is
/// no ledger yet), and how many of its lines are not a replay's. A replay's
/// calls are from a minute before it started to a minute after it
/// finished; one with no finish line (killed) runs until a minute after its
/// last key expired, but no longer than an STS session lasts, or for a day
/// with a key that doesn't expire.
fn read_ledger(path: &Path) -> std::io::Result<(Vec<Calls>, usize)> {
    let text = match std::fs::read_to_string(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        read => read?,
    };
    #[derive(Default)]
    struct Lines {
        keys: Vec<String>,
        started: Option<DateTime<Utc>>,
        expires: Option<DateTime<Utc>>,
        finished: Option<DateTime<Utc>>,
    }
    let (mut runs, mut bad) = (HashMap::<String, Lines>::new(), 0);
    // an empty line is where a torn one was ended
    for line in text.lines().filter(|l| !l.is_empty()) {
        let Some(line) = Line::parse(line) else {
            bad += 1;
            continue;
        };
        let run = runs.entry(line.id).or_default();
        run.keys.extend(line.key);
        run.started = run.started.or(line.started);
        run.expires = run.expires.max(line.expires);
        run.finished = run.finished.or(line.finished);
    }
    let calls = runs.into_iter().filter_map(|(id, run)| {
        let started = run.started?;
        let until = match (run.finished, run.expires) {
            (Some(t), _) => t + skew(),
            (None, Some(t)) => (t + skew()).min(started + longest_session()),
            (None, None) => started + TimeDelta::days(1),
        };
        Some(Calls {
            tag: tag(&id),
            keys: run.keys,
            from: started - skew(),
            until,
            left_out: 0,
        })
    });
    Ok((calls.collect(), bad))
}

/// A ledger line, as a replay writes it: an id of 16 hex digits, and any
/// access key ID (letters and digits) and times (RFC 3339 strings) it has.
/// Any other line is not a replay's: an id of another length could match
/// another replay's tag (an empty one, every replay's), and a time outside
/// RFC 3339's four-digit years could overflow a window.
struct Line {
    id: String,
    key: Option<String>,
    started: Option<DateTime<Utc>>,
    expires: Option<DateTime<Utc>>,
    finished: Option<DateTime<Utc>>,
}

impl Line {
    fn parse(line: &str) -> Option<Self> {
        let v: Value = serde_json::from_str(line).ok()?;
        let id = v["id"].as_str().filter(|id| {
            id.len() == 16 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        })?;
        let key = match &v["access_key_id"] {
            Value::Null => None,
            k => Some(
                k.as_str()
                    .filter(|k| !k.is_empty() && k.bytes().all(|b| b.is_ascii_alphanumeric()))?,
            ),
        };
        let time = |name: &str| match &v[name] {
            Value::Null => Some(None),
            t => Some(Some(
                DateTime::parse_from_rfc3339(t.as_str()?)
                    .ok()?
                    .with_timezone(&Utc),
            )),
        };
        Some(Self {
            id: id.to_string(),
            key: key.map(String::from),
            started: time("started")?,
            expires: time("key_expires")?,
            finished: time("finished")?,
        })
    }
}

/// A replay's LookupEvents calls, as a later read of Event history tells
/// them: signed with one of its access keys, at a time it ran, with its tag
/// in the user agent. The tag alone is not to be trusted, since anyone can
/// set a user agent and earlier replays' are in Event history; an access
/// key ID can't sign a call without its secret.
#[derive(Debug)]
struct Calls {
    /// `app/tangent-replay-<id>`.
    tag: String,
    keys: Vec<String>,
    from: DateTime<Utc>,
    until: DateTime<Utc>,
    /// Those read and left out.
    left_out: u64,
}

impl Calls {
    /// Whether `v`, at `t`, is one of them.
    fn matches(&self, v: &Value, t: Option<DateTime<Utc>>) -> bool {
        v["eventName"] == "LookupEvents"
            && t.is_some_and(|t| self.from <= t && t <= self.until)
            && v["userIdentity"]["accessKeyId"]
                .as_str()
                .is_some_and(|k| self.keys.iter().any(|key| key == k))
            && v["userAgent"]
                .as_str()
                .is_some_and(|ua| tagged(ua, &self.tag))
    }
}

/// Whether the user agent `ua` holds `tag` whole: not followed by another
/// hex digit, as a longer id would be. The SDK can put its build's metadata
/// (`md/...`) right after the app name, with no space.
fn tagged(ua: &str, tag: &str) -> bool {
    ua.match_indices(tag)
        .any(|(i, _)| !ua[i + tag.len()..].starts_with(|c: char| c.is_ascii_hexdigit()))
}

/// The SDK's identity cache, telling the run each access key its calls are
/// signed with. Asking the credentials provider instead would resolve them
/// again: for a role or SSO, a new session with a key of its own.
#[derive(Debug)]
struct Signs(SharedIdentityCache, Arc<Run>);

impl ResolveCachedIdentity for Signs {
    fn resolve_cached_identity<'a>(
        &'a self,
        resolver: SharedIdentityResolver,
        components: &'a RuntimeComponents,
        bag: &'a ConfigBag,
    ) -> IdentityFuture<'a> {
        IdentityFuture::new(async move {
            let identity = self
                .0
                .resolve_cached_identity(resolver, components, bag)
                .await?;
            if let Some(c) = identity.data::<Credentials>() {
                self.1.signs_with(c.access_key_id(), c.expiry());
            }
            Ok(identity)
        })
    }
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
/// for the run in its user agent and telling it the access keys its calls
/// are signed with; and the opt-in regions asked once already.
pub struct Clients(
    Vec<(&'static str, Client)>,
    parking_lot::Mutex<HashSet<&'static str>>,
);

impl Clients {
    pub fn new(aws: &aws_config::SdkConfig, run: &Arc<Run>) -> Result<Self> {
        let app = run.app();
        let app = AppName::new(app.clone()).map_err(|e| anyhow!("app name {app}: {e}"))?;
        let cache = SharedIdentityCache::new(Signs(
            aws.identity_cache()
                .unwrap_or_else(|| IdentityCache::lazy().build()),
            Arc::clone(run),
        ));
        Ok(Self(
            DEFAULT_REGIONS
                .into_iter()
                .chain(OPT_IN_REGIONS)
                .map(|r| {
                    let conf = aws_sdk_cloudtrail::config::Builder::from(aws)
                        .region(AwsRegion::new(r))
                        .app_name(app.clone())
                        .identity_cache(cache.clone())
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
    /// The replay reading it.
    run: Arc<Run>,
    /// The calls of earlier replays from this machine, left out too.
    earlier: Vec<Calls>,
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
    /// The history `[from, end)` through `lookup`, whose calls are the
    /// run's.
    pub fn new(
        lookup: Box<dyn Lookup>,
        from: DateTime<Utc>,
        end: DateTime<Utc>,
        run: Arc<Run>,
    ) -> Self {
        let now = Instant::now();
        Self {
            lookup,
            earlier: run.earlier(from, end),
            run,
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

    /// The LookupEvents calls left out, the replay's own and earlier
    /// replays': what to say, if any were.
    pub fn left_out(&self) -> Option<String> {
        let mut out = Vec::new();
        if self.own > 0 {
            out.push(format!("{} of the replay's own left out", self.own));
        }
        let runs = self.earlier.iter().filter(|r| r.left_out > 0).count();
        if runs > 0 {
            let calls: u64 = self.earlier.iter().map(|r| r.left_out).sum();
            let c = if calls == 1 { "" } else { "s" };
            let s = if runs == 1 { "" } else { "s" };
            out.push(format!(
                "{calls} LookupEvents call{c} of {runs} earlier replay{s} from this machine left out"
            ));
        }
        (!out.is_empty()).then(|| out.join("; "))
    }

    /// Every record before `until` not yet handed out, in time order, with
    /// its time. Once the range is read, it makes no more calls.
    pub async fn until(&mut self, until: DateTime<Utc>) -> Result<Vec<(DateTime<Utc>, Value)>> {
        let until = until.min(self.end);
        let mut out = Vec::new();
        while self.next < until {
            let end = (self.next + TimeDelta::hours(1)).min(until);
            out.extend(self.slice(self.next, end).await?);
            self.next = end;
        }
        if self.next >= self.end {
            self.stop();
        }
        Ok(out)
    }

    /// It is read no more: the replay's finish goes in the ledger, if it
    /// isn't there yet.
    pub fn stop(&self) {
        self.run.finished(Utc::now());
    }

    /// Every region's records in `[start, end)`, in time order, ties by
    /// `eventID`. One without an `eventTime` takes `start`. The replay's own
    /// calls are left out: the range ends when it started, but a clock ahead
    /// of AWS's puts its first calls before that. So are earlier replays'.
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

        let own = self.run.calls(Utc::now());
        let mut out = Vec::new();
        for raw in events {
            let v: Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("an event whose CloudTrailEvent is not JSON, skipped: {e}");
                    continue;
                }
            };
            let time = parse_time(&v["eventTime"]);
            let t = time.unwrap_or(start);
            if t < start || t >= end {
                continue;
            }
            if own.matches(&v, time) {
                self.own += 1;
                continue;
            }
            if let Some(run) = self.earlier.iter_mut().find(|r| r.matches(&v, time)) {
                run.left_out += 1;
                continue;
            }
            out.push((t, v));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| id(&a.1).cmp(&id(&b.1))));
        self.records += out.len() as u64;
        Ok(out)
    }
}

impl Drop for History {
    /// One not stopped yet (the replay failed or was stopped before it
    /// stepped through the range) is now.
    fn drop(&mut self) {
        self.stop();
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

/// The source's past for `tangent replay`: the Event history of the account
/// its credentials are in, every region's, for `[from, to)` up to when the
/// replay started.
pub(crate) async fn past(
    name: &str,
    cfg: &CloudtrailConfig,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<History> {
    let started = Utc::now();
    let end = range_end(from, to, started).with_context(|| format!("source {name}"))?;
    let aws = aws(cfg).await;
    if end < to {
        tracing::info!(source = %name, %end, "the range ends when the replay started: Event history after then holds the replay's own LookupEvents calls");
    }
    if end > started - lag() {
        tracing::warn!(source = %name, %end, "Event history may not have the range's last few minutes yet: CloudTrail typically has an event within 5 minutes of its call");
    }
    let run = Arc::new(Run::new(started, ledger_path()));
    let app = run.app();
    tracing::info!(source = %name, %from, %end, %app, "cloudtrail source reading Event history for a range");
    Ok(History::new(
        Box::new(Clients::new(&aws, &run)?),
        from,
        end,
        run,
    ))
}

#[async_trait]
impl Past for History {
    async fn until(&mut self, t: DateTime<Utc>) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        for (at, mut v) in History::until(self, t).await? {
            stamp(SOURCE, at, &mut v);
            out.push(v);
        }
        Ok(out)
    }
    fn size(&self) -> String {
        let mut out = format!("{} not enabled", self.not_enabled);
        if self.unreachable > 0 {
            out += &format!(", {} unreachable", self.unreachable);
        }
        format!(
            "Event history to {} in {} of {ALL_REGIONS} regions ({out}), management events only",
            self.end().format("%Y-%m-%dT%H:%M:%SZ"),
            self.regions(),
        )
    }
    fn estimate(&self) -> Option<u64> {
        History::estimate(self)
    }
    fn read(&self) -> String {
        let mut out = format!("from {} LookupEvents calls", self.calls);
        if let Some(left) = self.left_out() {
            out += &format!(" ({left})");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::parse_when;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use tracing_subscriber::layer::SubscriberExt;

    type Asked = Arc<Mutex<Vec<(&'static str, DateTime<Utc>, Instant)>>>;

    const ID: &str = "000000000000000a";
    const APP: &str = "tangent-replay-000000000000000a";
    /// The access key the replay's calls are signed with.
    const KEY: &str = "ASIAOWN";

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

    /// The replay `ID`, started at `started`, with the ledger at `ledger`.
    fn run(started: &str, ledger: Option<&Path>) -> Arc<Run> {
        Arc::new(Run {
            id: ID.into(),
            ..Run::new(at(started), ledger.map(Path::to_path_buf))
        })
    }

    fn history(lookup: impl Lookup + 'static, from: &str, end: &str) -> History {
        replay(lookup, from, end, run(end, None))
    }

    fn replay(lookup: impl Lookup + 'static, from: &str, end: &str, run: Arc<Run>) -> History {
        let mut h = History::new(Box::new(lookup), at(from), at(end), run);
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
        let clients = Clients::new(&aws, &run("2026-10-01T12:00:00Z", None)).unwrap();
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
        let mut h = History::new(
            Box::new(mem),
            at("2026-10-01T10:00:00Z"),
            end,
            run("2026-10-01T11:30:00.250Z", None),
        );
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

    /// A LookupEvents call at `time`, signed with `key`, tagged `app`.
    fn lookup_event(time: DateTime<Utc>, id: &str, key: &str, app: &str) -> Value {
        json!({
            "eventTime": time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "eventID": id,
            "eventSource": "cloudtrail.amazonaws.com",
            "eventName": "LookupEvents",
            "userIdentity": {"type": "AssumedRole", "accessKeyId": key},
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
            logged.push((region, lookup_event(*now, &format!("own-{n}"), KEY, APP)));
            *now += TimeDelta::milliseconds(10);
            Ok(Answer::Page(Page { events, next: None }))
        }
    }

    /// A clock ahead of AWS's puts the replay's first calls before the
    /// range's end; the last hour reads them back and leaves them out, but
    /// keeps an earlier replay's the ledger doesn't have.
    #[tokio::test]
    async fn a_clock_ahead_of_aws_reads_no_call_of_its_own() {
        let started = at("2026-10-01T12:00:00Z");
        let from = started - TimeDelta::hours(2);
        let end = range_end(from, started, started).unwrap();
        let earlier = lookup_event(
            at("2026-10-01T11:30:00Z"),
            "earlier",
            "ASIAEARLIER",
            "tangent-replay-000000000000000b",
        );
        let mem = Skewed {
            aws_now: Mutex::new(started - TimeDelta::seconds(3)),
            logged: Mutex::new(vec![("us-east-1", earlier)]),
        };
        let run = run("2026-10-01T12:00:00Z", None);
        run.signs_with(KEY, None);
        let mut h = History::new(Box::new(mem), from, end, run);
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
        let (a, b) = (Run::new(Utc::now(), None), Run::new(Utc::now(), None));
        assert_ne!(a.id, b.id);
        assert!(
            a.id.len() == 16 && a.id.chars().all(|c| c.is_ascii_hexdigit()),
            "{}",
            a.id
        );
        assert_eq!(a.app(), format!("tangent-replay-{}", a.id));
        assert!(AppName::new(a.app()).is_ok());
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
    /// agent, and the ledger has the key it was signed with.
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
        let dir = tempfile::tempdir().unwrap();
        let ledger = dir.path().join("replays.jsonl");
        let clients = Clients::new(&aws, &run("2026-10-01T12:00:00Z", Some(&ledger))).unwrap();
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
        assert_eq!(
            lines(&ledger),
            [
                json!({"id": ID, "access_key_id": "AKID", "started": "2026-10-01T12:00:00.000Z", "key_expires": null})
            ]
        );
    }

    const E: &str = "00000000000000e1";
    const F: &str = "00000000000000f2";

    /// The ledger's lines.
    fn lines(path: &Path) -> Vec<Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// An earlier replay `id` in the ledger at `path`: started at `started`,
    /// signing with `keys` in turn (each with when it expires, if it does),
    /// and finished at `finished`, if it was.
    fn earlier(
        path: &Path,
        id: &str,
        started: &str,
        keys: &[(&str, Option<&str>)],
        finished: Option<&str>,
    ) -> Run {
        let run = Run {
            id: id.into(),
            ..Run::new(at(started), Some(path.to_path_buf()))
        };
        for (key, expires) in keys {
            run.signs_with(key, expires.map(|t| at(t).into()));
        }
        if let Some(t) = finished {
            run.finished(at(t));
        }
        run
    }

    /// The window and keys the ledger has for the replay `id`.
    fn window(path: &Path, id: &str) -> (DateTime<Utc>, DateTime<Utc>, Vec<String>) {
        let (runs, bad) = read_ledger(path).unwrap();
        assert_eq!(bad, 0);
        let c = runs.into_iter().find(|c| c.tag == tag(id)).unwrap();
        (c.from, c.until, c.keys)
    }

    /// The ledger at `path` holds `lines`, as written by hand.
    fn write_ledger(path: &Path, lines: &[Value]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(path, text).unwrap();
    }

    /// Replays 10:00 to 12:00 with the ledger at `path`, over `events`:
    /// the ids kept, and what it says it left out.
    async fn kept(path: &Path, events: Vec<Value>) -> (Vec<String>, Option<String>) {
        let mem = Mem {
            events: HashMap::from([("us-east-1", events)]),
            ..Mem::default()
        };
        let mut h = replay(
            mem,
            "2026-10-01T10:00:00Z",
            "2026-10-01T12:00:00Z",
            run("2026-10-01T12:00:00Z", Some(path)),
        );
        let got = h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
        let ids = ids(&got).into_iter().map(String::from).collect();
        (ids, h.left_out())
    }

    /// An earlier replay's call is left out only when it is a LookupEvents
    /// call, signed with that replay's key, while it ran, and tagged with
    /// its app name: its tag with another key, or its key and tag at
    /// another time, or another call are kept.
    #[tokio::test]
    async fn an_earlier_replays_calls_are_left_out_by_key_time_and_tag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tangent/replays.jsonl");
        // an IAM user's key from 10:00 to 10:30, a role's from 10:40 to 10:50
        earlier(
            &path,
            E,
            "2026-10-01T10:00:00Z",
            &[("AKIAE", None)],
            Some("2026-10-01T10:30:00Z"),
        );
        earlier(
            &path,
            F,
            "2026-10-01T10:40:00Z",
            &[("ASIAF", Some("2026-10-01T11:40:00Z"))],
            Some("2026-10-01T10:50:00Z"),
        );
        let (e, f) = (format!("tangent-replay-{E}"), format!("tangent-replay-{F}"));
        let mut other = lookup_event(at("2026-10-01T10:15:00Z"), "other-name", "AKIAE", &e);
        other["eventName"] = "DescribeTrails".into();
        let mem = Mem {
            events: HashMap::from([(
                "us-east-1",
                vec![
                    lookup_event(at("2026-10-01T10:05:00Z"), "e1", "AKIAE", &e),
                    // a minute after it finished: the clocks may be that far apart
                    lookup_event(at("2026-10-01T10:31:00Z"), "e2", "AKIAE", &e),
                    lookup_event(at("2026-10-01T10:45:00Z"), "f1", "ASIAF", &f),
                    // its tag, as Event history shows it, with another key
                    lookup_event(at("2026-10-01T10:10:00Z"), "forged", "AKIAATTACKER", &e),
                    // its key and tag, after it finished
                    lookup_event(at("2026-10-01T10:31:01Z"), "late", "AKIAE", &e),
                    other,
                    // one replay's key, the other's tag
                    lookup_event(at("2026-10-01T10:46:00Z"), "crossed", "ASIAF", &e),
                ],
            )]),
            ..Mem::default()
        };
        let mut h = replay(
            mem,
            "2026-10-01T10:00:00Z",
            "2026-10-01T12:00:00Z",
            run("2026-10-01T12:00:00Z", Some(&path)),
        );
        let got = h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
        assert_eq!(ids(&got), ["forged", "other-name", "late", "crossed"]);
        assert_eq!(h.own, 0);
        assert_eq!(
            h.left_out().unwrap(),
            "3 LookupEvents calls of 2 earlier replays from this machine left out"
        );
    }

    /// The replay's own calls go by the same rule: its key, since it
    /// started (less a minute), and its tag.
    #[tokio::test]
    async fn the_replays_own_calls_are_left_out_by_key_time_and_tag() {
        let r = run("2026-10-01T12:00:00Z", None);
        r.signs_with(KEY, None);
        let mut other = lookup_event(at("2026-10-01T11:59:30Z"), "other-name", KEY, APP);
        other["eventName"] = "DescribeTrails".into();
        let mem = Mem {
            events: HashMap::from([(
                "us-east-1",
                vec![
                    lookup_event(at("2026-10-01T11:59:30Z"), "own", KEY, APP),
                    lookup_event(at("2026-10-01T11:59:30Z"), "forged", "AKIAATTACKER", APP),
                    lookup_event(at("2026-10-01T11:58:59Z"), "before", KEY, APP),
                    // its key, as another tool on the machine uses it
                    lookup_event(at("2026-10-01T11:59:30Z"), "untagged", KEY, "aws-cli"),
                    other,
                ],
            )]),
            ..Mem::default()
        };
        let mut h = replay(mem, "2026-10-01T11:00:00Z", "2026-10-01T12:00:00Z", r);
        let got = h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
        assert_eq!(ids(&got), ["before", "forged", "other-name", "untagged"]);
        assert_eq!(h.own, 1);
        assert_eq!(h.left_out().unwrap(), "1 of the replay's own left out");
    }

    /// A replay killed before its finish line counts as running until a
    /// minute after its key expires, or for a day with a key that doesn't.
    #[test]
    fn a_killed_replay_runs_until_its_key_expires_or_for_a_day() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replays.jsonl");
        let role = [("ASIAE", Some("2026-10-01T11:00:00Z"))];
        earlier(&path, E, "2026-10-01T10:00:00Z", &role, None);
        earlier(&path, F, "2026-10-01T10:00:00Z", &[("AKIAF", None)], None);
        let (from, until, _) = window(&path, E);
        assert_eq!(
            (from, until),
            (at("2026-10-01T09:59:00Z"), at("2026-10-01T11:01:00Z"))
        );
        let (from, until, _) = window(&path, F);
        assert_eq!(
            (from, until),
            (at("2026-10-01T09:59:00Z"), at("2026-10-02T10:00:00Z"))
        );
    }

    /// A key the SDK refreshes to goes in the ledger as it is first used,
    /// so a later replay leaves out the calls it signed, and a replay
    /// killed after it runs until the new key expires.
    #[tokio::test]
    async fn a_key_refreshed_mid_run_is_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replays.jsonl");
        let r = earlier(
            &path,
            E,
            "2026-10-01T10:00:00Z",
            &[
                ("ASIA1", Some("2026-10-01T10:55:00Z")),
                ("ASIA1", Some("2026-10-01T10:55:00Z")),
                ("ASIA2", Some("2026-10-01T11:55:00Z")),
            ],
            None,
        );
        assert_eq!(lines(&path).len(), 2, "a key once");
        assert_eq!(
            window(&path, E),
            (
                at("2026-10-01T09:59:00Z"),
                at("2026-10-01T11:56:00Z"),
                vec!["ASIA1".to_string(), "ASIA2".to_string()]
            )
        );
        r.finished(at("2026-10-01T11:10:00Z"));
        assert_eq!(window(&path, E).1, at("2026-10-01T11:11:00Z"));

        let e = format!("tangent-replay-{E}");
        let mem = Mem {
            events: HashMap::from([(
                "us-east-1",
                vec![
                    lookup_event(at("2026-10-01T10:30:00Z"), "first", "ASIA1", &e),
                    lookup_event(at("2026-10-01T11:05:00Z"), "refreshed", "ASIA2", &e),
                    event("2026-10-01T11:06:00Z", "kept"),
                ],
            )]),
            ..Mem::default()
        };
        let mut h = replay(
            mem,
            "2026-10-01T10:00:00Z",
            "2026-10-01T12:00:00Z",
            run("2026-10-01T12:00:00Z", Some(&path)),
        );
        let got = h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
        assert_eq!(ids(&got), ["kept"]);
        assert_eq!(
            h.left_out().unwrap(),
            "2 LookupEvents calls of 1 earlier replay from this machine left out"
        );
    }

    /// The clients against a stand-in for CloudTrail, with credentials that
    /// expire within the SDK's refresh buffer, so each call gets a new key:
    /// the ledger has each key a call was signed with, in turn, the first
    /// with the start. A History dropped before it read its range (as when
    /// the replay fails) writes the finish.
    #[cfg(unix)]
    #[tokio::test]
    async fn clients_record_each_key_the_sdk_signs_with() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let signed = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&signed);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 16 * 1024];
                let n = sock.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let key = req
                    .split("Credential=")
                    .nth(1)
                    .and_then(|c| c.split('/').next());
                seen.lock()
                    .unwrap()
                    .push(key.unwrap_or_default().to_string());
                let body = r#"{"Events":[]}"#;
                let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                sock.write_all(resp.as_bytes()).await.unwrap();
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let (script, count) = (dir.path().join("creds.sh"), dir.path().join("n"));
        // within the SDK's 10 s refresh buffer
        let expires = (Utc::now() + TimeDelta::seconds(5)).format("%Y-%m-%dT%H:%M:%SZ");
        std::fs::write(
            &script,
            format!(
                "n=$(($(cat \"$1\" 2>/dev/null || echo 0) + 1)); echo $n > \"$1\"\nprintf '{{\"Version\":1,\"AccessKeyId\":\"ASIAKEY%s\",\"SecretAccessKey\":\"s\",\"SessionToken\":\"t\",\"Expiration\":\"{expires}\"}}' $n\n"
            ),
        )
        .unwrap();
        let provider = aws_config::credential_process::CredentialProcessProvider::new(format!(
            "sh {} {}",
            script.display(),
            count.display()
        ));
        let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(AwsRegion::new("us-east-1"))
            .credentials_provider(provider)
            .endpoint_url(format!("http://{addr}"))
            .load()
            .await;
        let ledger = dir.path().join("tangent/replays.jsonl");
        let r = run("2026-10-01T12:00:00Z", Some(&ledger));
        let clients = Clients::new(&aws, &r).unwrap();
        let (start, end) = (at("2026-10-01T10:00:00Z"), at("2026-10-01T11:00:00Z"));
        // an opt-in region's first call is made with a config override
        for region in ["af-south-1", "us-east-1", "us-east-1", "eu-west-1"] {
            let got = clients.lookup(region, start, end, None).await.unwrap();
            assert!(matches!(got, Answer::Page(_)), "{region}");
        }
        let signed = signed.lock().unwrap().clone();
        assert_eq!(signed, ["ASIAKEY1", "ASIAKEY2", "ASIAKEY3", "ASIAKEY4"]);
        let written = lines(&ledger);
        let recorded: Vec<&str> = written
            .iter()
            .filter_map(|v| v["access_key_id"].as_str())
            .collect();
        assert_eq!(recorded, signed, "each key signed with, in turn");
        assert!(written[0]["started"].is_string(), "{}", written[0]);

        let h = History::new(Box::new(clients), start, end, r);
        drop(h);
        let written = lines(&ledger);
        assert_eq!(written.len(), 5);
        assert!(written[4]["finished"].is_string(), "{}", written[4]);
    }

    /// A History writes its finish once its range is read, as it makes no
    /// more calls then; a stop after that, or its drop, writes no other.
    #[tokio::test]
    async fn the_finish_goes_in_once_the_range_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replays.jsonl");
        let r = run("2026-10-01T12:00:00Z", Some(&path));
        r.signs_with(KEY, None);
        let mut h = replay(
            Mem::default(),
            "2026-10-01T10:00:00Z",
            "2026-10-01T12:00:00Z",
            r,
        );
        h.until(at("2026-10-01T11:00:00Z")).await.unwrap();
        assert_eq!(lines(&path).len(), 1, "the start only, with an hour to go");
        h.until(at("2026-10-01T13:00:00Z")).await.unwrap();
        let finished = lines(&path);
        assert_eq!(finished.len(), 2);
        assert_eq!(finished[1]["id"], ID);
        assert!(finished[1]["finished"].is_string(), "{}", finished[1]);
        h.stop();
        drop(h);
        assert_eq!(lines(&path), finished);
    }

    /// A replay killed before its finish line counts as running no longer
    /// than an STS session lasts, whatever its key's expiry says: a line
    /// can't leave a key's calls out for good.
    #[tokio::test]
    async fn a_killed_replays_window_is_at_most_36_hours() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tangent/replays.jsonl");
        let id = "1111111111111111";
        write_ledger(
            &path,
            &[
                json!({"id": id, "access_key_id": "AKIAOLD", "started": "2020-01-01T00:00:00Z", "key_expires": "9999-12-31T23:59:59.999Z"}),
            ],
        );
        assert_eq!(
            window(&path, id),
            (
                at("2019-12-31T23:59:00Z"),
                at("2020-01-02T12:00:00Z"),
                vec!["AKIAOLD".to_string()]
            )
        );
        let ua = format!("tangent-replay-{id}");
        let call = lookup_event(at("2026-10-01T11:00:00Z"), "recon", "AKIAOLD", &ua);
        assert_eq!(kept(&path, vec![call]).await.0, ["recon"]);
    }

    /// Only a line as a replay writes it counts: an id of 16 lowercase hex
    /// digits (an empty one would match every replay's tag), an access key
    /// ID of letters and digits, and RFC 3339 times (an epoch number at the
    /// end of chrono's range would overflow the window).
    #[tokio::test]
    async fn a_line_a_replay_would_not_write_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tangent/replays.jsonl");
        let end_of_time = DateTime::<Utc>::MAX_UTC.timestamp_millis() - 999;
        let line = |id: &str, key: &str| json!({"id": id, "access_key_id": key, "started": "2026-10-01T09:00:00Z", "finished": "2026-10-01T11:50:00Z"});
        write_ledger(
            &path,
            &[
                line("", "AKIASHARED"),
                line("abc", "AKIASHARED"),
                line("ABCDEFABCDEF0123", "AKIASHARED"),
                line("abcdefabcdef01234", "AKIASHARED"),
                line("1111111111111111", ""),
                line("1111111111111111", "AKIA SHARED"),
                json!({"id": "2222222222222222", "access_key_id": "AKIASHARED", "started": "2026-10-01T09:00:00Z", "key_expires": end_of_time}),
                json!({"id": "3333333333333333", "access_key_id": "AKIASHARED", "started": 1790000000}),
                json!({"id": "4444444444444444", "access_key_id": "AKIASHARED", "started": "+262143-12-31T23:59:59Z"}),
            ],
        );
        let (runs, bad) = read_ledger(&path).unwrap();
        assert!(runs.is_empty(), "{runs:?}");
        assert_eq!(bad, 9);
        let other = lookup_event(
            at("2026-10-01T11:00:00Z"),
            "other-machine",
            "AKIASHARED",
            "tangent-replay-abcdefabcdef0123",
        );
        assert_eq!(
            kept(&path, vec![other]).await,
            (vec!["other-machine".to_string()], None)
        );
    }

    /// The tag goes by itself: one with more hex digits after it is
    /// another's, but the SDK's build metadata right after it is not.
    #[tokio::test]
    async fn the_tag_is_matched_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tangent/replays.jsonl");
        earlier(
            &path,
            E,
            "2026-10-01T10:00:00Z",
            &[("AKIAE", None)],
            Some("2026-10-01T11:50:00Z"),
        );
        let e = format!("tangent-replay-{E}");
        let (got, left) = kept(
            &path,
            vec![
                lookup_event(
                    at("2026-10-01T11:00:00Z"),
                    "longer",
                    "AKIAE",
                    &format!("{e}ffff"),
                ),
                lookup_event(
                    at("2026-10-01T11:01:00Z"),
                    "md",
                    "AKIAE",
                    &format!("{e}md/x"),
                ),
            ],
        )
        .await;
        assert_eq!(got, ["longer"]);
        assert_eq!(
            left.unwrap(),
            "1 LookupEvents call of 1 earlier replay from this machine left out"
        );
    }

    /// A last line left torn (a crash, a full disk) is ended before the
    /// next replay's start line, so that one is read whole.
    #[test]
    fn a_torn_line_does_not_take_the_next_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replays.jsonl");
        std::fs::write(&path, "{\"id\": \"00000000000000c3\", \"acc").unwrap();
        earlier(
            &path,
            E,
            "2026-10-01T10:00:00Z",
            &[("AKIAE", None)],
            Some("2026-10-01T10:30:00Z"),
        );
        let (runs, bad) = read_ledger(&path).unwrap();
        assert_eq!(bad, 1, "the torn line");
        assert_eq!(runs.len(), 1);
        assert_eq!(
            (runs[0].from, runs[0].until, runs[0].tag.as_str()),
            (
                at("2026-10-01T09:59:00Z"),
                at("2026-10-01T10:31:00Z"),
                tag(E).as_str()
            )
        );
    }
    /// What a replay writes is what a later one reads, in a directory and
    /// file only the user can read; a line that isn't a replay's is skipped.
    #[test]
    fn ledger_lines_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tangent/replays.jsonl");
        // a replay that signed no call writes nothing
        earlier(
            &path,
            F,
            "2026-10-01T09:00:00Z",
            &[],
            Some("2026-10-01T09:01:00Z"),
        );
        assert!(!path.exists());
        let keys = [
            (KEY, Some("2026-10-01T11:00:00Z")),
            ("ASIA2", Some("2026-10-01T12:00:00Z")),
        ];
        earlier(
            &path,
            ID,
            "2026-10-01T10:00:00.250Z",
            &keys,
            Some("2026-10-01T11:30:00Z"),
        );
        assert_eq!(
            lines(&path),
            [
                json!({"id": ID, "access_key_id": KEY, "started": "2026-10-01T10:00:00.250Z", "key_expires": "2026-10-01T11:00:00.000Z"}),
                json!({"id": ID, "access_key_id": "ASIA2", "key_expires": "2026-10-01T12:00:00.000Z"}),
                json!({"id": ID, "finished": "2026-10-01T11:30:00.000Z"}),
            ]
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(path.parent().unwrap()), 0o700);
            assert_eq!(mode(&path), 0o600);
        }
        assert_eq!(
            window(&path, ID),
            (
                at("2026-10-01T09:59:00.250Z"),
                at("2026-10-01T11:31:00Z"),
                vec![KEY.to_string(), "ASIA2".to_string()]
            )
        );

        append(&path, &json!({"finished": "2026-10-01T11:30:00Z"})).unwrap();
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(b"{\"id\": \"00000000000000c3\", \"acc")
            .unwrap();
        let (runs, bad) = read_ledger(&path).unwrap();
        assert_eq!((runs.len(), bad), (1, 2));
    }

    /// How many warnings are logged on this thread while the guard lives.
    fn warnings() -> (tracing::subscriber::DefaultGuard, Arc<AtomicUsize>) {
        struct Count(Arc<AtomicUsize>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Count {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if *event.metadata().level() == tracing::Level::WARN {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        let n = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(Count(Arc::clone(&n)));
        (tracing::subscriber::set_default(subscriber), n)
    }

    /// A ledger that can't be read or written is warned of once, and the
    /// replay goes on, leaving out its own calls still.
    #[tokio::test]
    async fn a_ledger_it_cannot_read_or_write_warns_once() {
        let dir = tempfile::tempdir().unwrap();
        let read_only = dir.path().join("read-only.jsonl");
        earlier(
            &read_only,
            E,
            "2026-10-01T10:00:00Z",
            &[("AKIAE", None)],
            None,
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o400);
            std::fs::set_permissions(&read_only, perms).unwrap();
        }
        let writable = std::fs::OpenOptions::new()
            .append(true)
            .open(&read_only)
            .is_ok();
        // a directory where the ledger would be: neither read nor written
        let neither = dir.path().join("dir.jsonl");
        std::fs::create_dir(&neither).unwrap();

        for (path, earlier_kept) in [(&neither, true), (&read_only, false)] {
            if path == &read_only && writable {
                continue; // as root, a read-only file is written anyway
            }
            let (_guard, warned) = warnings();
            let r = run("2026-10-01T12:00:00Z", Some(path));
            let mem = Mem {
                events: HashMap::from([(
                    "us-east-1",
                    vec![
                        lookup_event(
                            at("2026-10-01T10:05:00Z"),
                            "e1",
                            "AKIAE",
                            &format!("tangent-replay-{E}"),
                        ),
                        lookup_event(at("2026-10-01T11:59:30Z"), "own", KEY, APP),
                        event("2026-10-01T11:06:00Z", "kept"),
                    ],
                )]),
                ..Mem::default()
            };
            let mut h = replay(
                mem,
                "2026-10-01T10:00:00Z",
                "2026-10-01T12:00:00Z",
                Arc::clone(&r),
            );
            r.signs_with(KEY, None);
            let got = h.until(at("2026-10-01T12:00:00Z")).await.unwrap();
            let want: &[&str] = if earlier_kept {
                &["e1", "kept"]
            } else {
                &["kept"]
            };
            assert_eq!(ids(&got), want, "{}", path.display());
            assert_eq!(h.own, 1);
            drop(h);
            assert_eq!(warned.load(Ordering::SeqCst), 1, "{}", path.display());
        }
    }
}
