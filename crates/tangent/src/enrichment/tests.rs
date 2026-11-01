//! The three table types, what a missing or a broken file does, and a plugin
//! on two workers, beside another plugin, reading tables loaded once.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ipnet::IpNet;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tangent_shared::enrichment::{TableConfig, TableKind};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;

use super::{Cidr, Table, Tables};

/// How many times each file was read into a table, across every test in
/// the process; each test uses files of its own.
static LOADS: Lazy<Mutex<HashMap<PathBuf, usize>>> = Lazy::new(Default::default);

pub(super) fn count_load(path: &Path) {
    *LOADS.lock().entry(path.to_path_buf()).or_default() += 1;
}

fn loads(path: &Path) -> usize {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    LOADS.lock().get(&path).copied().unwrap_or(0)
}

/// What `f` returns, and how many warnings it logged.
fn warnings<T>(f: impl FnOnce() -> T) -> (T, usize) {
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
    let out = tracing::subscriber::with_default(subscriber, f);
    (out, n.load(Ordering::SeqCst))
}

fn table(kind: TableKind, path: &Path) -> TableConfig {
    TableConfig {
        kind,
        path: path.to_path_buf(),
        required: false,
    }
}

fn tables(entries: &[(&str, TableConfig)]) -> anyhow::Result<Tables> {
    let cfgs: BTreeMap<Arc<str>, TableConfig> = entries
        .iter()
        .map(|(n, c)| (Arc::from(*n), c.clone()))
        .collect();
    Tables::load(&cfgs, Path::new("/"))
}

fn parsed(answer: Option<String>) -> Value {
    answer.map_or(Value::Null, |a| serde_json::from_str(&a).unwrap())
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

const NETWORKS: &str = "\
# networks: one prefix a line
prefix,label
203.0.113.0/24,aws
203.0.113.128/25, datacenter
203.0.113.200,tor
2001:db8::/32,gcp
2001:db8:1::/48,datacenter
198.51.100.0/24,vpn
198.51.100.0/25,vpn

10.1.2.3/8,office
ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff,top
";

#[test]
fn cidr_answers_every_label_that_holds_an_address() {
    let c = Cidr::parse(NETWORKS.as_bytes()).unwrap();
    let labels = |a: &str| c.lookup(ip(a)).map(|s| s.to_string());
    assert_eq!(labels("203.0.113.7").as_deref(), Some(r#"["aws"]"#));
    assert_eq!(
        labels("203.0.113.130").as_deref(),
        Some(r#"["aws","datacenter"]"#),
        "a prefix inside another: both"
    );
    assert_eq!(
        labels("203.0.113.200").as_deref(),
        Some(r#"["aws","datacenter","tor"]"#),
        "a bare address is its own /32"
    );
    assert_eq!(
        labels("203.0.113.255").as_deref(),
        Some(r#"["aws","datacenter"]"#)
    );
    assert_eq!(labels("203.0.114.0"), None);
    assert_eq!(labels("203.0.112.255"), None);
    assert_eq!(
        labels("198.51.100.3").as_deref(),
        Some(r#"["vpn"]"#),
        "two prefixes of one label: the label once"
    );
    assert_eq!(labels("198.51.100.200").as_deref(), Some(r#"["vpn"]"#));
    assert_eq!(
        labels("10.255.255.255").as_deref(),
        Some(r#"["office"]"#),
        "host bits cleared"
    );
    assert_eq!(labels("11.0.0.0"), None);
}

#[test]
fn cidr_answers_ipv6_and_mapped_ipv4() {
    let c = Cidr::parse(NETWORKS.as_bytes()).unwrap();
    let labels = |a: &str| c.lookup(ip(a)).map(|s| s.to_string());
    assert_eq!(labels("2001:db8::1").as_deref(), Some(r#"["gcp"]"#));
    assert_eq!(
        labels("2001:db8:1::5").as_deref(),
        Some(r#"["datacenter","gcp"]"#),
        "in the order the labels first appear in the file, not the prefixes'"
    );
    assert_eq!(labels("2001:db8:ffff::1").as_deref(), Some(r#"["gcp"]"#));
    assert_eq!(labels("2001:db9::1"), None);
    assert_eq!(
        labels("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").as_deref(),
        Some(r#"["top"]"#),
        "the last address there is"
    );
    let t = Table::Cidr(c);
    assert_eq!(
        t.lookup("::ffff:203.0.113.7").as_deref(),
        Some(r#"["aws"]"#)
    );
    assert_eq!(t.lookup(" 203.0.113.7 ").as_deref(), Some(r#"["aws"]"#));
    assert_eq!(t.lookup("not an address"), None);
    assert_eq!(t.lookup(""), None);
    assert_eq!(
        t.lookup("203.0.113.7:443"),
        None,
        "an address, not a socket"
    );
}

#[test]
fn cidr_with_every_address_in_a_prefix() {
    let c = Cidr::parse(b"0.0.0.0/0,v4\n::/0,any\n").unwrap();
    assert_eq!(c.lookup(ip("192.0.2.1")), Some(r#"["v4"]"#));
    assert_eq!(c.lookup(ip("255.255.255.255")), Some(r#"["v4"]"#));
    assert_eq!(c.lookup(ip("2001:db8::1")), Some(r#"["any"]"#));
    assert_eq!(c.prefixes(), 2);
    let empty = Cidr::parse(b"# nothing yet\n").unwrap();
    assert_eq!(empty.lookup(ip("192.0.2.1")), None);
}

#[test]
fn an_ipv6_prefix_never_holds_an_ipv4_address() {
    // ::/8 is in bogon lists; IPv4 mapped into IPv6 would sit inside it
    let c = Cidr::parse(b"::/8,x\n::/0,y\n::ffff:0:0/95,z\n").unwrap();
    for a in ["8.8.8.8", "::ffff:8.8.8.8", "0.0.0.0", "255.255.255.255"] {
        assert_eq!(c.lookup(ip(a)), None, "{a}");
    }
    assert_eq!(c.lookup(ip("2001:db8::1")), Some(r#"["y"]"#));
    assert_eq!(c.lookup(ip("::1")), Some(r#"["x","y"]"#));
    assert_eq!(c.lookup(ip("::fffe:0:1")), Some(r#"["x","y","z"]"#));
}

#[test]
fn an_ipv4_mapped_prefix_is_the_ipv4_prefix_it_maps() {
    let c =
        Cidr::parse(b"::ffff:203.0.113.0/120,mapped\n::ffff:198.51.100.7,one\n::ffff:0:0/96,all\n")
            .unwrap();
    assert_eq!(c.lookup(ip("203.0.113.7")), Some(r#"["mapped","all"]"#));
    assert_eq!(
        c.lookup(ip("::ffff:203.0.113.7")),
        Some(r#"["mapped","all"]"#)
    );
    assert_eq!(c.lookup(ip("198.51.100.7")), Some(r#"["one","all"]"#));
    assert_eq!(c.lookup(ip("198.51.100.8")), Some(r#"["all"]"#));
    assert_eq!(c.lookup(ip("2001:db8::1")), None);
}

#[test]
fn a_malformed_cidr_file_is_an_error_naming_the_table_the_file_and_the_line() {
    let dir = tempfile::tempdir().unwrap();
    for (text, says) in [
        (
            "203.0.113.0/24,aws\n203.0.113.0/24\n",
            "line 2: want `prefix,label`",
        ),
        (
            "# ranges\nnot-a-net,aws\n",
            "line 2: `not-a-net` is not an address",
        ),
        ("203.0.113.0/33,aws\n", "line 1: `203.0.113.0/33`"),
        ("203.0.113.0/24,\n", "line 1: `203.0.113.0/24` has no label"),
    ] {
        let path = dir.path().join("networks.csv");
        std::fs::write(&path, text).unwrap();
        let err = match tables(&[("networks", table(TableKind::Cidr, &path))]) {
            Ok(_) => panic!("{text:?} loaded"),
            Err(e) => format!("{e:#}"),
        };
        assert!(err.contains("enrichment table networks"), "{err}");
        assert!(err.contains("networks.csv is not a cidr file"), "{err}");
        assert!(err.contains(says), "{err}");
    }
}

/// A value in a MaxMind DB, for `mmdb` below.
enum D {
    Str(&'static str),
    U16(u16),
    U32(u32),
    U64(u64),
    F64(f64),
    Bool(bool),
    Map(Vec<(&'static str, D)>),
    Arr(Vec<D>),
}

/// A MaxMind DB holding each `(prefix, record)`, laid out as MaxMind's are:
/// an IPv6 tree with IPv4 under `::a.b.c.d`, 32-bit records. The format is
/// at https://maxmind.github.io/MaxMind-DB/; no prefix may hold another.
fn mmdb(entries: Vec<(&str, D)>) -> Vec<u8> {
    #[derive(Clone, Copy)]
    enum Rec {
        Node(u32),
        Empty,
        Data(u32),
    }
    let mut nodes: Vec<[Rec; 2]> = vec![[Rec::Empty, Rec::Empty]];
    let mut data = Vec::new();
    for (prefix, record) in entries {
        let (bits, len) = match prefix.parse::<IpNet>().unwrap() {
            IpNet::V4(n) => (u128::from(u32::from(n.network())), 96 + n.prefix_len()),
            IpNet::V6(n) => (u128::from(n.network()), n.prefix_len()),
        };
        let at = data.len() as u32;
        encode(&record, &mut data);
        let mut node = 0;
        for depth in 0..len {
            let bit = ((bits >> (127 - depth)) & 1) as usize;
            if depth == len - 1 {
                nodes[node][bit] = Rec::Data(at);
                break;
            }
            node = match nodes[node][bit] {
                Rec::Node(n) => n as usize,
                _ => {
                    nodes.push([Rec::Empty, Rec::Empty]);
                    let n = nodes.len() - 1;
                    nodes[node][bit] = Rec::Node(n as u32);
                    n
                }
            };
        }
    }
    let count = nodes.len() as u32;
    let mut out = Vec::new();
    for node in &nodes {
        for rec in node {
            let v = match *rec {
                Rec::Node(n) => n,
                Rec::Empty => count,
                Rec::Data(at) => count + 16 + at,
            };
            out.extend(v.to_be_bytes());
        }
    }
    out.extend([0u8; 16]);
    out.extend(data);
    out.extend(b"\xAB\xCD\xEFMaxMind.com");
    encode(
        &D::Map(vec![
            ("binary_format_major_version", D::U16(2)),
            ("binary_format_minor_version", D::U16(0)),
            ("build_epoch", D::U64(1_700_000_000)),
            ("database_type", D::Str("Test-Geo")),
            ("description", D::Map(vec![("en", D::Str("for tests"))])),
            ("ip_version", D::U16(6)),
            ("languages", D::Arr(vec![D::Str("en")])),
            ("node_count", D::U32(count)),
            ("record_size", D::U16(32)),
        ]),
        &mut out,
    );
    out
}

fn encode(v: &D, out: &mut Vec<u8>) {
    fn head(kind: u8, size: usize, out: &mut Vec<u8>) {
        let (bits, extra): (u8, Vec<u8>) = match size {
            0..=28 => (size as u8, vec![]),
            29..=284 => (29, vec![(size - 29) as u8]),
            _ => (30, ((size - 285) as u16).to_be_bytes().to_vec()),
        };
        if kind <= 7 {
            out.push(kind << 5 | bits);
        } else {
            out.push(bits);
            out.push(kind - 7);
        }
        out.extend(extra);
    }
    fn uint(kind: u8, n: u64, out: &mut Vec<u8>) {
        let bytes = n.to_be_bytes();
        let skip = bytes.iter().take_while(|b| **b == 0).count();
        head(kind, 8 - skip, out);
        out.extend(&bytes[skip..]);
    }
    match v {
        D::Str(s) => {
            head(2, s.len(), out);
            out.extend(s.as_bytes());
        }
        D::F64(f) => {
            head(3, 8, out);
            out.extend(f.to_be_bytes());
        }
        D::U16(n) => uint(5, u64::from(*n), out),
        D::U32(n) => uint(6, u64::from(*n), out),
        D::U64(n) => uint(9, *n, out),
        D::Bool(b) => head(14, usize::from(*b), out),
        D::Map(entries) => {
            head(7, entries.len(), out);
            for (k, v) in entries {
                encode(&D::Str(k), out);
                encode(v, out);
            }
        }
        D::Arr(items) => {
            head(11, items.len(), out);
            for item in items {
                encode(item, out);
            }
        }
    }
}

fn geo_db() -> Vec<u8> {
    mmdb(vec![
        (
            "203.0.113.0/24",
            D::Map(vec![
                ("country", D::Map(vec![("iso_code", D::Str("ZZ"))])),
                ("autonomous_system_number", D::U32(64500)),
                ("autonomous_system_organization", D::Str("Example Hosting")),
                ("anycast", D::Bool(true)),
                ("score", D::F64(0.5)),
                ("tags", D::Arr(vec![D::Str("a"), D::Str("b")])),
            ]),
        ),
        (
            "2001:db8::/32",
            D::Map(vec![("country", D::Map(vec![("iso_code", D::Str("YY"))]))]),
        ),
    ])
}

#[test]
fn mmdb_answers_an_address_with_its_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("geo.mmdb");
    std::fs::write(&path, geo_db()).unwrap();
    let t = tables(&[("geo", table(TableKind::Mmdb, &path))]).unwrap();
    assert_eq!(
        parsed(t.lookup("geo", "203.0.113.7")),
        json!({
            "country": {"iso_code": "ZZ"},
            "autonomous_system_number": 64500,
            "autonomous_system_organization": "Example Hosting",
            "anycast": true,
            "score": 0.5,
            "tags": ["a", "b"],
        })
    );
    assert_eq!(
        parsed(t.lookup("geo", "2001:db8:5::1")),
        json!({"country": {"iso_code": "YY"}}),
        "IPv6"
    );
    assert_eq!(
        parsed(t.lookup("geo", "::ffff:203.0.113.9"))["country"]["iso_code"],
        "ZZ",
        "IPv4-mapped IPv6 is the IPv4 address"
    );
    assert_eq!(t.lookup("geo", "198.51.100.1"), None);
    assert_eq!(t.lookup("geo", "2001:db9::1"), None);
    assert_eq!(t.lookup("geo", "ZZ"), None);
}

#[test]
fn a_malformed_mmdb_is_an_error_naming_the_table_and_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("GeoLite2-Country.mmdb");
    std::fs::write(&path, b"<html>rate limited</html>").unwrap();
    let err = match tables(&[("geo", table(TableKind::Mmdb, &path))]) {
        Ok(_) => panic!("loaded"),
        Err(e) => format!("{e:#}"),
    };
    assert!(err.contains("enrichment table geo"), "{err}");
    assert!(
        err.contains("GeoLite2-Country.mmdb is not a mmdb file"),
        "{err}"
    );
}

const PROFILE: &str = "
org: acme
networks: [10.0.0.0/8, 203.0.113.0/24]
accounts:
  111122223333: prod
domains:
  acme.example: { owner: it }
people:
  - { name: ana, sensitive: true }
unset:
";

#[test]
fn a_document_answers_a_dotted_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("profile.yaml");
    std::fs::write(&path, PROFILE).unwrap();
    let t = tables(&[("profile", table(TableKind::Document, &path))]).unwrap();
    let get = |k: &str| t.lookup("profile", k);
    assert_eq!(get("org").as_deref(), Some(r#""acme""#));
    assert_eq!(
        parsed(get("networks")),
        json!(["10.0.0.0/8", "203.0.113.0/24"])
    );
    assert_eq!(get("networks[1]").as_deref(), Some(r#""203.0.113.0/24""#));
    assert_eq!(get("networks.0").as_deref(), Some(r#""10.0.0.0/8""#));
    assert_eq!(
        get("accounts.111122223333").as_deref(),
        Some(r#""prod""#),
        "a key YAML reads as a number is its text"
    );
    assert_eq!(
        get("domains.acme.example.owner").as_deref(),
        Some(r#""it""#),
        "a key with dots in it"
    );
    assert_eq!(get("people[0].sensitive").as_deref(), Some("true"));
    assert_eq!(parsed(get(""))["org"], "acme", "\"\" is all of it");
    assert_eq!(get("unset"), None, "a null is nothing");
    assert_eq!(get("missing"), None);
    assert_eq!(get("org.name"), None);
    assert_eq!(get("networks[9]"), None);

    let json_path = dir.path().join("profile.json");
    std::fs::write(&json_path, r#"{"org": "acme", "watch": ["root"]}"#).unwrap();
    let t = tables(&[("p", table(TableKind::Document, &json_path))]).unwrap();
    assert_eq!(t.lookup("p", "watch").as_deref(), Some(r#"["root"]"#));
}

#[test]
fn a_malformed_document_is_an_error_naming_the_table_and_the_file() {
    let dir = tempfile::tempdir().unwrap();
    for (name, text) in [
        ("profile.yaml", "org: [acme\n"),
        ("profile.json", "{\"org\": }"),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, text).unwrap();
        let err = match tables(&[("profile", table(TableKind::Document, &path))]) {
            Ok(_) => panic!("{name} loaded"),
            Err(e) => format!("{e:#}"),
        };
        assert!(err.contains("enrichment table profile"), "{err}");
        assert!(err.contains(&format!("{name} is not a document")), "{err}");
    }
}

#[test]
fn a_missing_file_warns_once_and_answers_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data/GeoLite2-ASN.mmdb");
    let (t, warned) = warnings(|| tables(&[("asn", table(TableKind::Mmdb, &path))]));
    let t = t.unwrap();
    assert_eq!(warned, 1, "once, at load");
    let (answers, warned) = warnings(|| {
        (0..3)
            .map(|_| t.lookup("asn", "203.0.113.7"))
            .collect::<Vec<_>>()
    });
    assert_eq!(answers, [None, None, None]);
    assert_eq!(warned, 0, "not again per lookup");
    assert!(matches!(**t.get("asn").unwrap(), Table::Missing));
}

#[test]
fn a_required_table_without_its_file_is_an_error_naming_both() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("profle/profile.yaml");
    let required = TableConfig {
        required: true,
        ..table(TableKind::Document, &path)
    };
    for entries in [
        vec![("profile", required.clone())],
        // the same file, read first for a table that may go without it
        vec![
            ("any", table(TableKind::Document, &path)),
            ("profile", required),
        ],
    ] {
        let err = match tables(&entries) {
            Ok(_) => panic!("loaded"),
            Err(e) => format!("{e:#}"),
        };
        assert!(err.contains("enrichment table profile: "), "{err}");
        assert!(
            err.contains("profle/profile.yaml not found, and the table is required"),
            "{err}"
        );
    }
}

#[test]
fn a_table_no_config_defines_warns_once_per_name() {
    let t = Tables::default();
    let (answers, warned) = warnings(|| {
        vec![
            t.lookup("geoo", "203.0.113.7"),
            t.lookup("geoo", "198.51.100.1"),
            t.lookup("asn", "203.0.113.7"),
        ]
    });
    assert_eq!(answers, [None, None, None]);
    assert_eq!(warned, 2);
}

#[test]
fn a_file_two_tables_name_is_read_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("networks.csv");
    std::fs::write(&path, NETWORKS).unwrap();
    let t = tables(&[
        ("networks", table(TableKind::Cidr, &path)),
        ("ranges", table(TableKind::Cidr, &path)),
        (
            "raw",
            table(TableKind::Document, &dir.path().join("x.yaml")),
        ),
    ])
    .unwrap();
    assert_eq!(loads(&path), 1);
    assert!(Arc::ptr_eq(
        t.get("networks").unwrap(),
        t.get("ranges").unwrap()
    ));
    assert_eq!(
        t.lookup("ranges", "203.0.113.7").as_deref(),
        Some(r#"["aws"]"#)
    );
}

#[test]
fn a_relative_path_is_read_against_the_config_directory() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("data")).unwrap();
    std::fs::write(dir.path().join("data/networks.csv"), NETWORKS).unwrap();
    let cfgs: BTreeMap<Arc<str>, TableConfig> = [(
        Arc::from("networks"),
        table(TableKind::Cidr, Path::new("data/networks.csv")),
    )]
    .into();
    let t = Tables::load(&cfgs, dir.path()).unwrap();
    assert_eq!(
        t.lookup("networks", "2001:db8::1").as_deref(),
        Some(r#"["gcp"]"#)
    );
}

const LOOKUP: &str = include_str!("../../testdata/enrichment/lookup.component.wat");
const PLAIN: &str = include_str!("../../testdata/enrichment/plain.component.wat");

/// `lookup` twice (plugins `a` and `b`) and `plain`, a plugin built without
/// the enrichment interface, on two workers, from one config: every
/// record reaches all three, both lookups answer from tables read once, and
/// every engine and plugin instance holds the same tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugins_on_every_worker_read_tables_loaded_once() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::write(root.join("data/geo.mmdb"), geo_db()).unwrap();
    std::fs::write(root.join("data/networks.csv"), NETWORKS).unwrap();

    let engine = tangent_shared::wasm_engine::build().unwrap();
    for (name, wat) in [("a", LOOKUP), ("b", LOOKUP), ("plain", PLAIN)] {
        let cwasm = engine.precompile_component(wat.as_bytes()).unwrap();
        std::fs::write(root.join(format!("plugins/{name}.cwasm")), cwasm).unwrap();
    }

    let ips = ["203.0.113.7", "2001:db8::1", "198.51.100.1"];
    let mut input = String::new();
    for i in 0..60 {
        match ips.get(i % 4) {
            Some(ip) => input.push_str(&format!("{{\"id\":{i},\"ip\":\"{ip}\"}}\n")),
            None => input.push_str(&format!("{{\"id\":{i}}}\n")),
        }
    }
    std::fs::write(root.join("in.ndjson"), &input).unwrap();

    let config = format!(
        "
runtime:
  workers: 2
  batch_size: 1
  state: {{ backend: lru }}
enrichment_tables:
  geo: {{ type: mmdb, path: data/geo.mmdb }}
  networks: {{ type: cidr, path: data/networks.csv }}
plugins:
  a: {{ module_type: rust, path: . }}
  b: {{ module_type: rust, path: . }}
  plain: {{ module_type: rust, path: . }}
sources:
  in:
    type: file
    path: {root}/in.ndjson
    decoding: {{ format: {{ type: ndjson }} }}
sinks:
  out: {{ type: file, path: {root}/out.ndjson, compression: {{ type: none }} }}
dag:
  - from: {{ kind: source, name: in }}
    to: [{{ kind: plugin, name: a }}, {{ kind: plugin, name: b }}, {{ kind: plugin, name: plain }}]
  - from: {{ kind: plugin, name: a }}
    to: [{{ kind: sink, name: out }}]
  - from: {{ kind: plugin, name: b }}
    to: [{{ kind: sink, name: out }}]
  - from: {{ kind: plugin, name: plain }}
    to: [{{ kind: sink, name: out }}]
",
        root = root.display()
    );
    let cfg_path = root.join("tangent.yaml");
    std::fs::write(&cfg_path, config).unwrap();
    let cfg = tangent_shared::Config::from_file(&cfg_path).unwrap();

    let shutdown = CancellationToken::new();
    let rt =
        crate::dag::DagRuntime::build(cfg, &cfg_path, shutdown.clone(), CancellationToken::new())
            .await
            .unwrap();
    let tables = Arc::clone(&rt.tables);
    assert_eq!(loads(&root.join("data/geo.mmdb")), 1);
    assert_eq!(loads(&root.join("data/networks.csv")), 1);
    let (workers, plugins) = (2, 3);
    assert_eq!(
        Arc::strong_count(&tables),
        1 + 1 + workers + workers * plugins,
        "this test's, the runtime's, each worker's engine's, each plugin instance's: one Tables"
    );

    shutdown.cancel();
    rt.shutdown(Duration::from_secs(30), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(Arc::strong_count(&tables), 1, "nothing else keeps them");

    let out = std::fs::read_to_string(root.join("out.ndjson")).unwrap();
    let lines: Vec<Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let (looked, passed): (Vec<&Value>, Vec<&Value>) =
        lines.iter().partition(|l| l.get("geo").is_some());
    assert_eq!(passed.len(), 60, "plain passed every record through");
    assert!(passed.iter().all(|l| l.get("id").is_some()));
    assert_eq!(looked.len(), 120, "a and b each looked up every record");
    let mut by_ip: BTreeMap<String, (usize, Value, Value)> = BTreeMap::new();
    for l in looked {
        let e = by_ip
            .entry(l["ip"].as_str().unwrap().to_string())
            .or_insert((0, l["geo"].clone(), l["networks"].clone()));
        e.0 += 1;
        assert_eq!((&e.1, &e.2), (&l["geo"], &l["networks"]), "{l}");
    }
    assert_eq!(by_ip["203.0.113.7"].0, 30);
    assert_eq!(by_ip["203.0.113.7"].1["country"]["iso_code"], "ZZ");
    assert_eq!(by_ip["203.0.113.7"].2, json!(["aws"]));
    assert_eq!(by_ip["2001:db8::1"].1["country"]["iso_code"], "YY");
    assert_eq!(by_ip["2001:db8::1"].2, json!(["gcp"]));
    assert_eq!(
        (&by_ip["198.51.100.1"].1, &by_ip["198.51.100.1"].2),
        (&Value::Null, &json!(["vpn"]))
    );
    assert_eq!(
        (by_ip[""].0, &by_ip[""].1, &by_ip[""].2),
        (30, &Value::Null, &Value::Null),
        "a record with no address: nothing to look up"
    );
}
