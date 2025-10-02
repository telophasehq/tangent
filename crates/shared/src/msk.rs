use secrecy::SecretString;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MSKConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    #[serde(default = "default_group")]
    pub group_id: String,

    #[serde(default = "default_protocol")]
    pub security_protocol: String,

    #[serde(default)]
    pub ssl_ca_location: Option<String>,
    #[serde(default)]
    pub ssl_certificate_location: Option<String>,
    #[serde(default)]
    pub ssl_key_location: Option<String>,

    pub auth: MSKAuth,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum MSKAuth {
    Scram {
        #[serde(default = "default_scram_mech")]
        sasl_mechanism: String,
        username: String,
        password: SecretString,
    },
}

fn default_group() -> String {
    "tangent-node".into()
}
fn default_protocol() -> String {
    "PLAINTEXT".into()
}
fn default_scram_mech() -> String {
    "SCRAM-SHA-512".into()
}
