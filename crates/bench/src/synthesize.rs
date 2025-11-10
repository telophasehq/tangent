use rand::Rng;
use serde_json::Value;

use anyhow::{bail, Context, Result};
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

/// Per-thread generator with a seeded RNG and counters.
pub struct Synth {
    rng: ChaCha8Rng,
    counters: HashMap<String, i64>,
}

impl Synth {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            counters: HashMap::new(),
        }
    }

    pub fn gen(&mut self, spec: &Value, scope: &mut Scope) -> Result<Value> {
        match spec {
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Ok(spec.clone()),
            Value::Array(a) => {
                let mut out = Vec::with_capacity(a.len());
                for v in a {
                    out.push(self.gen(v, scope)?);
                }
                Ok(Value::Array(out))
            }
            Value::Object(obj) => {
                if obj.len() == 1 {
                    if let Some((k, v)) = obj.iter().next() {
                        if k.starts_with('$') {
                            return self.eval_op(k, v, scope);
                        }
                    }
                }
                let mut out = serde_json::Map::with_capacity(obj.len());
                for (k, v) in obj {
                    let val = self.gen(v, scope.with_path(k))?;
                    out.insert(k.clone(), val);
                }
                Ok(Value::Object(out))
            }
        }
    }

    fn eval_op(&mut self, op: &str, arg: &Value, scope: &mut Scope) -> Result<Value> {
        match op {
            "$const" => Ok(arg.clone()),
            "$keep" => Ok(scope.template_at()?),
            "$ref" => Ok(scope.lookup_ref(arg.as_str().context("$ref expects string path")?)?),

            "$oneOf" => {
                let arr = arg.as_array().context("$oneOf expects array")?;
                let idx = self.rng.random_range(0..arr.len());
                self.gen(&arr[idx], scope)
            }

            "$weighted" => {
                let arr = arg.as_array().context("$weighted expects array")?;
                let mut total = 0.0f64;
                let mut weights = Vec::with_capacity(arr.len());
                for pair in arr {
                    let p = pair
                        .as_array()
                        .context("$weighted item must be [value, weight]")?;
                    let w = p.get(1).and_then(Value::as_f64).context("missing weight")?;
                    weights.push((p.get(0).unwrap(), w));
                    total += w;
                }
                let mut pick = self.rng.random::<f64>() * total;
                for (v, w) in weights.iter() {
                    if pick <= *w {
                        return self.gen(v, scope);
                    }
                    pick -= w;
                }
                self.gen(weights.last().unwrap().0, scope)
            }

            "$int" => {
                let o = arg.as_object().context("$int expects object {min,max}")?;
                let min = o.get("min").and_then(Value::as_i64).unwrap_or(0);
                let max = o
                    .get("max")
                    .and_then(Value::as_i64)
                    .unwrap_or(min.max(0) + 100);
                Ok(Value::from(self.rng.random_range(min..=max)))
            }

            "$float" => {
                let o = arg.as_object().context("$float expects object {min,max}")?;
                let min = o.get("min").and_then(Value::as_f64).unwrap_or(0.0);
                let max = o.get("max").and_then(Value::as_f64).unwrap_or(min + 1.0);
                Ok(Value::from(self.rng.random_range(min..=max)))
            }

            "$normal" => {
                let o = arg.as_object().context("$normal expects {mean,stdev}")?;
                let mean = o.get("mean").and_then(Value::as_f64).context("mean")?;
                let stdev = o.get("stdev").and_then(Value::as_f64).context("stdev")?;
                let mut x = gaussian(&mut self.rng) * stdev + mean;
                if let Some(min) = o.get("min").and_then(Value::as_f64) {
                    x = x.max(min);
                }
                if let Some(max) = o.get("max").and_then(Value::as_f64) {
                    x = x.min(max);
                }
                Ok(Value::from(x))
            }

            "$string" => {
                let o = arg.as_object().context("$string expects {len,alphabet?}")?;
                let len = o.get("len").and_then(Value::as_u64).unwrap_or(8) as usize;
                let alpha = o
                    .get("alphabet")
                    .and_then(Value::as_str)
                    .unwrap_or("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");
                let chars: Vec<char> = alpha.chars().collect();
                let s: String = (0..len)
                    .map(|_| {
                        let i = self.rng.random_range(0..chars.len());
                        chars[i]
                    })
                    .collect();
                Ok(Value::from(s))
            }

            "$uuid" => Ok(Value::from(uuid::Uuid::new_v4().to_string())),

            "$now" => {
                let fmt = arg
                    .get("format")
                    .and_then(Value::as_str)
                    .unwrap_or("rfc3339");
                let now = chrono::Utc::now();
                let v = match fmt {
                    "unix_ms" => Value::from(now.timestamp_millis()),
                    "unix" => Value::from(now.timestamp()),
                    _ => Value::from(now.to_rfc3339()),
                };
                Ok(v)
            }

            "$ip4" => {
                let a: [u8; 4] = self.rng.random();
                Ok(Value::from(format!("{}.{}.{}.{}", a[0], a[1], a[2], a[3])))
            }

            "$inc" => {
                let o = arg.as_object().context("$inc expects {start,step}")?;
                let name = scope.path.clone(); // counter per field path
                let step = o.get("step").and_then(Value::as_i64).unwrap_or(1);
                let entry = self
                    .counters
                    .entry(name)
                    .or_insert(o.get("start").and_then(Value::as_i64).unwrap_or(0));
                let out = *entry;
                *entry = entry.saturating_add(step);
                Ok(Value::from(out))
            }

            "$array" => {
                let o = arg.as_object().context("$array expects {of,len}")?;
                let of = o.get("of").context("array.of missing")?;

                let one = Value::from(1);
                let len_val = o.get("len").unwrap_or(&one);
                let n = if let Some(n) = len_val.as_u64() {
                    n as usize
                } else {
                    self.gen(len_val, scope)?.as_u64().unwrap_or(1) as usize
                };
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    out.push(self.gen(of, scope)?);
                }
                Ok(Value::from(out))
            }

            "$map" => {
                let o = arg.as_object().context("$map expects {of:{...}}")?;
                let of = o
                    .get("of")
                    .and_then(Value::as_object)
                    .context("map.of missing")?;
                let mut m = serde_json::Map::with_capacity(of.len());
                for (k, v) in of {
                    m.insert(k.clone(), self.gen(v, scope.with_path(k))?);
                }
                Ok(Value::from(m))
            }

            "$rangeFrom" => {
                let o = arg.as_object().context("$rangeFrom expects {base,pct}")?;
                let base_v = o.get("base").context("base")?;
                let mut base = match base_v {
                    Value::String(s) if s.starts_with("$ref:") => {
                        let p = &s[5..];
                        scope.lookup_ref(p)?.as_f64().unwrap_or(0.0)
                    }
                    _ => self.gen(base_v, scope)?.as_f64().unwrap_or(0.0),
                };
                let pct = o.get("pct").and_then(Value::as_f64).context("pct")?;
                let jitter = self.rng.random_range(-pct..=pct);
                base *= 1.0 + jitter;
                Ok(Value::from(base))
            }

            "$gte1" => {
                let mut v = self.gen(arg, scope)?;
                if v.as_i64().map(|x| x < 1).unwrap_or(false) {
                    v = Value::from(1);
                }
                Ok(v)
            }

            "$let" => {
                let o = arg.as_object().context("$let expects {vars,in}")?;
                let vars = o.get("vars").and_then(Value::as_object).context("vars")?;
                let mut inner = scope.child();
                for (k, v) in vars {
                    let val = self.gen(v, &mut inner)?;
                    inner.bindings.insert(k.clone(), val);
                }
                let body = o.get("in").context("in")?;
                self.gen(body, &mut inner)
            }

            "$fmt" => {
                let o = arg.as_object().context("$fmt expects {template,vars}")?;
                let tpl = o
                    .get("template")
                    .and_then(Value::as_str)
                    .context("template")?;

                let empty_map = serde_json::Map::new();
                let vars = o
                    .get("vars")
                    .and_then(Value::as_object)
                    .unwrap_or(&empty_map);
                let mut map = HashMap::new();
                for (k, v) in vars {
                    let val = if let Some(s) = v.as_str() {
                        if s.starts_with("$ref:") {
                            scope.lookup_ref(&s[5..])?
                        } else {
                            self.gen(v, scope)?
                        }
                    } else {
                        self.gen(v, scope)?
                    };
                    map.insert(k.as_str(), val);
                }
                let out = interpolate(tpl, &map);
                Ok(Value::from(out))
            }

            other => bail!("unknown op: {other}"),
        }
    }
}
pub struct Scope<'a> {
    root_template: &'a Value,
    path: String,
    bindings: HashMap<String, Value>,
}
impl<'a> Scope<'a> {
    pub fn new(root: &'a Value) -> Self {
        Self {
            root_template: root,
            path: String::new(),
            bindings: HashMap::new(),
        }
    }
    pub fn with_path(&mut self, seg: &str) -> &mut Self {
        if self.path.is_empty() {
            self.path = seg.to_string();
        } else {
            self.path.push('.');
            self.path.push_str(seg);
        }
        self
    }
    pub fn child(&self) -> Scope<'a> {
        Scope {
            root_template: self.root_template,
            path: String::new(),
            bindings: self.bindings.clone(),
        }
    }
    pub fn lookup_ref(&self, p: &str) -> Result<Value> {
        if let Some(v) = self.bindings.get(p) {
            return Ok(v.clone());
        }
        let mut cur = self.root_template;
        for seg in p.split('.') {
            cur = cur
                .get(seg)
                .with_context(|| format!("$ref path not found: {p}"))?;
        }
        Ok(cur.clone())
    }
    pub fn template_at(&self) -> Result<Value> {
        if self.path.is_empty() {
            return Ok(self.root_template.clone());
        }
        self.lookup_ref(&self.path)
    }
}

fn gaussian<R: Rng>(rng: &mut R) -> f64 {
    // Box–Muller
    let u1: f64 = rng.random::<f64>().max(f64::MIN_POSITIVE);
    let u2: f64 = rng.random();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

fn interpolate(tpl: &str, vars: &HashMap<&str, Value>) -> String {
    let mut out = String::with_capacity(tpl.len() + 16);
    let mut i = 0;
    while let Some(start) = tpl[i..].find('{') {
        out.push_str(&tpl[i..i + start]);
        let end = tpl[i + start + 1..].find('}').unwrap_or(0) + i + start + 1;
        let key = &tpl[i + start + 1..end];
        if let Some(v) = vars.get(key) {
            out.push_str(&v.as_str().unwrap_or(&v.to_string()));
        }
        i = end + 1;
    }
    out.push_str(&tpl[i..]);
    out
}
