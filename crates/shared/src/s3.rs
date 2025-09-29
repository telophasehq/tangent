use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub bucket_name: String,
    pub region: Option<String>,
}
