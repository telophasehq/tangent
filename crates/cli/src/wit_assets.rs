use include_dir::{include_dir, Dir};

pub static WIT_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/../../assets/wit");
