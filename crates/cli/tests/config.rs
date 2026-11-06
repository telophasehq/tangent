//! `tangent config` as it is run: its warnings go to stderr, which no test
//! of [`tangent_shared::Config::warnings`] sees.

use std::process::Command;

/// A sink no edge reaches: `tangent config` warns of it on stderr, keeps
/// stdout YAML, and exits 0, since the rest of the config runs.
#[test]
fn config_warns_of_a_node_no_edge_reaches() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("tangent.yaml");
    std::fs::write(
        &file,
        "sources:\n  input: { type: file, path: in.ndjson, decoding: { format: { type: ndjson } } }\n\
         sinks:\n  out: { type: blackhole }\n  never: { type: blackhole }\n\
         dag:\n  - from: input\n    to: [out]\n",
    )
    .unwrap();
    let shown = Command::new(env!("CARGO_BIN_EXE_tangent"))
        .arg("config")
        .arg("--config")
        .arg(&file)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&shown.stdout);
    let stderr = String::from_utf8_lossy(&shown.stderr);
    assert!(shown.status.success(), "{stderr}");
    assert!(
        stderr.contains("warning: sink never: no edge reaches it; it never runs"),
        "{stderr}"
    );
    assert!(!stdout.contains("warning"), "{stdout}");
}
