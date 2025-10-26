use regex::Regex;

use crate::wasm::{
    host::JsonLogView,
    host::{
        exports::tangent::logs::mapper::{self, Pred},
        tangent::logs::log,
    },
};
use CmpScalar::{Bool, Bytes, Float, Int, Str};

#[derive(Clone)]
enum CmpScalar {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl From<log::Scalar> for CmpScalar {
    fn from(s: log::Scalar) -> Self {
        match s {
            log::Scalar::Str(x) => Self::Str(x),
            log::Scalar::Int(x) => Self::Int(x),
            log::Scalar::Float(x) => Self::Float(x),
            log::Scalar::Boolean(x) => Self::Bool(x),
            log::Scalar::Bytes(bs) => Self::Bytes(bs),
        }
    }
}

enum PredOp {
    Has { path: String },
    Eq { path: String, rhs: CmpScalar },
    Prefix { path: String, prefix: String },
    In { path: String, set: Vec<CmpScalar> },
    Gt { path: String, rhs: f64 },
    Re { path: String, re: Regex },
}

pub struct CompiledSelector {
    any: Vec<PredOp>,
    all: Vec<PredOp>,
    none: Vec<PredOp>,
}

pub fn compile_selector(sel: &mapper::Selector) -> anyhow::Result<CompiledSelector> {
    let mut cs = CompiledSelector {
        any: vec![],
        all: vec![],
        none: vec![],
    };

    let conv = |p: &Pred| -> anyhow::Result<PredOp> {
        Ok(match p {
            Pred::Has(path) => PredOp::Has { path: path.clone() },
            Pred::Eq((path, s)) => PredOp::Eq {
                path: path.clone(),
                rhs: s.clone().into(),
            },
            Pred::Prefix((path, pre)) => PredOp::Prefix {
                path: path.clone(),
                prefix: pre.clone(),
            },
            Pred::In((path, list)) => PredOp::In {
                path: path.clone(),
                set: list.iter().cloned().map(Into::into).collect(),
            },
            Pred::Gt((path, rhs)) => PredOp::Gt {
                path: path.clone(),
                rhs: *rhs,
            },
            Pred::Regex((path, re)) => PredOp::Re {
                path: path.clone(),
                re: Regex::new(re)?,
            },
        })
    };

    for p in &sel.any {
        cs.any.push(conv(p)?);
    }
    for p in &sel.all {
        cs.all.push(conv(p)?);
    }
    for p in &sel.none {
        cs.none.push(conv(p)?);
    }
    Ok(cs)
}

fn eval_pred(pred: &PredOp, view: &JsonLogView) -> bool {
    match pred {
        PredOp::Has { path } => view.lookup(path).is_some(),

        PredOp::Eq { path, rhs } => {
            let val = view.lookup(path).and_then(JsonLogView::to_scalar);
            val.is_some_and(|s| {
                let s: CmpScalar = s.into();
                match (s, rhs) {
                    (Str(a), Str(b)) => a == *b,
                    (Int(a), Int(b)) => a == *b,
                    (Float(a), Float(b)) => (a - b).abs() < f64::EPSILON,
                    (Bool(a), Bool(b)) => a == *b,
                    (Bytes(a), Bytes(b)) => a == *b,
                    (Int(a), Float(b)) => (a as f64 - b).abs() < f64::EPSILON,
                    (Float(a), Int(b)) => (a - *b as f64).abs() < f64::EPSILON,
                    _ => false,
                }
            })
        }

        PredOp::Prefix { path, prefix } => {
            let val = view.lookup(path).and_then(JsonLogView::to_scalar);
            matches!(val, Some(log::Scalar::Str(s)) if s.starts_with(prefix))
        }

        PredOp::In { path, set } => {
            let val = view.lookup(path).and_then(JsonLogView::to_scalar);
            val.is_some_and(|val| {
                let v: CmpScalar = val.into();
                set.iter().any(|rhs| match (&v, rhs) {
                    (Str(a), Str(b)) => a == b,
                    (Int(a), Int(b)) => a == b,
                    (Float(a), Float(b)) => (a - b).abs() < f64::EPSILON,
                    (Bool(a), Bool(b)) => a == b,
                    (Bytes(a), Bytes(b)) => a == b,
                    (Int(a), Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
                    (Float(a), Int(b)) => (a - *b as f64).abs() < f64::EPSILON,
                    _ => false,
                })
            })
        }

        PredOp::Gt { path, rhs } => {
            let val = view.lookup(path).and_then(JsonLogView::to_scalar);
            match val {
                Some(log::Scalar::Int(i)) => (i as f64) > *rhs,
                Some(log::Scalar::Float(f)) => f > *rhs,
                _ => false,
            }
        }

        PredOp::Re { path, re } => {
            let out = view.lookup(path).and_then(JsonLogView::to_scalar);
            matches!(out, Some(log::Scalar::Str(s)) if re.is_match(&s))
        }
    }
}

pub fn eval_selector(sel: &CompiledSelector, v: &JsonLogView) -> bool {
    // ANY
    if !sel.any.is_empty() {
        let mut ok = false;
        for p in &sel.any {
            if eval_pred(p, v) {
                ok = true;
                break;
            }
        }
        if !ok {
            return false;
        }
    }
    // ALL
    for p in &sel.all {
        if !eval_pred(p, v) {
            return false;
        }
    }
    // NONE
    for p in &sel.none {
        if eval_pred(p, v) {
            return false;
        }
    }
    true
}
