//! Any JSON records: encoded, decoded, the same records; encoded again, the same text.
#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use tangent_compact::{decode_all, encode, CompactConfig};

fn config(sidecar: bool, budget_tokens: usize) -> CompactConfig {
    let mut c: CompactConfig = serde_yaml::from_str(
        "{keep: [_context, _norm.actor], sources: {
           cloudtrail: {principal: [_norm.actor, userIdentity.arn], time: [eventTime, ts], keep: [eventName, requestParameters], ids: [eventID, requestID]},
           okta: {principal: [actor.alternateId], time: [published], keep: [eventType], ids: [uuid]}}}",
    )
    .unwrap();
    c.sidecar = sidecar;
    c.budget_tokens = budget_tokens;
    c
}

fuzz_target!(|data: &[u8]| {
    let (mode, json) = data.split_first().unwrap_or((&0, data));
    let records = match serde_json::from_slice::<Value>(json) {
        Ok(Value::Array(a)) if a.iter().all(Value::is_object) => a,
        Ok(v @ Value::Object(_)) => vec![v],
        _ => return,
    };
    let c = config(mode & 1 == 1, if mode & 2 == 2 { 64 } else { 90_000 });
    let chunks = encode(&records, &c).expect("a JSON object encodes");
    for k in &chunks {
        assert!(
            k.text.chars().all(|ch| ch == '\n' || !ch.is_control()),
            "a raw control character in the text"
        );
    }
    let back = decode_all(chunks.iter().map(|k| (k.text.as_str(), k.side.as_deref())))
        .expect("its own text decodes");
    assert_eq!(back, records);
    let again = encode(&records, &c).unwrap();
    assert!(chunks
        .iter()
        .zip(&again)
        .all(|(a, b)| a.text == b.text && a.side == b.side));
});
