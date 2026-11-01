use normkit::{empty, kept, obj, pick, shrink};
use serde_json::{Map, Value};

pub fn cloudtrail(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(
        m,
        &[
            "ts",
            "source",
            "eventTime",
            "eventSource",
            "eventName",
            "awsRegion",
            "recipientAccountId",
            "sourceIPAddress",
            "userAgent",
            "errorCode",
            "errorMessage",
            "readOnly",
            "resources",
        ],
    );
    if let Some(u) = obj(m.get("userIdentity")) {
        let mut id = pick(u, &["type", "userName", "arn", "accountId", "invokedBy"]);
        let uv = Value::Object(u.clone());
        if let Some(role) = uv.pointer("/sessionContext/sessionIssuer/userName") {
            id.insert("role".into(), role.clone());
        }
        if let Some(mfa) = uv.pointer("/sessionContext/attributes/mfaAuthenticated") {
            id.insert("mfaAuthenticated".into(), mfa.clone());
        }
        out.insert("userIdentity".into(), Value::Object(id));
    }
    // scrubbed after the size gates, so a scrub only ever takes away
    let rp = m.get("requestParameters").filter(|rp| !rp.is_null());
    if let Some(rp) = rp.and_then(|rp| scrubbed(shrink(rp, 1500), |_| {})) {
        out.insert("requestParameters".into(), rp);
    }
    let name = m.get("eventName").and_then(Value::as_str).unwrap_or("");
    let granting = name == "ConsoleLogin"
        || name.starts_with("AssumeRole")
        || name.starts_with("Create")
        || name == "GetSessionToken"
        || name == "GetFederationToken";
    let re = m
        .get("responseElements")
        .filter(|re| !re.is_null() && (granting || re.to_string().len() <= 300));
    if let Some(re) = re.and_then(|re| scrubbed(shrink(re, 1000), |re| unissued(re, name))) {
        out.insert("responseElements".into(), re);
    }
    if let Some(a) = obj(m.get("additionalEventData")) {
        let keep = pick(
            a,
            &["MFAUsed", "LoginTo", "SamlProviderArn", "CredentialType"],
        );
        if !keep.is_empty() {
            out.insert("additionalEventData".into(), Value::Object(keep));
        }
    }
    out
}

/// Keys no verdict turns on, at any depth of a call's parameters or response.
const UNSEEN: &[&str] = &[
    "nextToken",
    "NextToken",
    "marker",
    "Marker",
    "nextMarker",
    "NextMarker",
    "paginationToken",
    "PaginationToken",
    "continuationToken",
    "ContinuationToken",
    "clientToken",
    "ClientToken",
    "idempotencyToken",
    "IdempotencyToken",
    "callerReference",
    "CallerReference",
    "requestId",
    "RequestId",
    "uploadId",
    "UploadId",
    "streamUrl",
    "sessionToken",
    "SessionToken",
    "tokenValue",
    "accessToken",
    "AccessToken",
    "idToken",
    "IdToken",
    "refreshToken",
    "RefreshToken",
    "secretAccessKey",
    "SecretAccessKey",
    "secretKey",
    "challengeParameters",
    "challengeResponses",
    "authParameters",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "itemContentHash",
    "contentHash",
    "ifMatch",
    "IfMatch",
    "eTag",
    "ETag",
    "layerDigest",
    "layerDigests",
    "imageManifest",
    "aws-crypto-public-key",
    "sourceAAD",
    "destinationAAD",
    "packedPolicySize",
    "sessionTokenUtilization",
    "sessionTokenSize",
    "messageSchemaVersion",
    "schemaVersion",
    "x-id",
];

fn unissued(re: &mut Map<String, Value>, name: &str) {
    let issued = |k: &str| matches!(k, "credentials" | "Credentials" | "roleCredentials");
    re.retain(|k, _| !issued(k));
    if let Some(u) = re.get_mut("assumedRoleUser").and_then(Value::as_object_mut) {
        u.remove("assumedRoleId");
    }
    // shrink's names, less those cut anyway and assumedRoleUser (roleArn and roleSessionName)
    if let Some(o) = re.get_mut("_omitted").and_then(Value::as_array_mut) {
        let cut = |n: &str| issued(n) || n == "assumedRoleUser" || UNSEEN.contains(&n);
        o.retain(|n| !n.as_str().is_some_and(cut));
        if o.is_empty() {
            re.remove("_omitted");
        }
    }
    if name == "CreateInvalidation" {
        re.remove("location");
        if let Some(i) = re.get_mut("invalidation").and_then(Value::as_object_mut) {
            i.remove("id");
        }
    }
}

fn scrubbed(mut v: Value, cut: impl FnOnce(&mut Map<String, Value>)) -> Option<Value> {
    let was = empty(&v);
    if let Some(m) = v.as_object_mut() {
        cut(m);
    }
    kept(&mut v, &|k, _| UNSEEN.contains(&k));
    (was || !empty(&v)).then_some(v)
}
