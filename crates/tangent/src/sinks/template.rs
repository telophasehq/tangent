use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Utc};
use std::fmt::Write as _;
use std::sync::Arc;

/// A strftime template, parsed once when a config loads and rendered in
/// UTC for each write: a rolling_file sink's `path` below its root, an s3
/// sink's `key_prefix`.
#[derive(Debug)]
pub struct Template {
    items: Vec<Item<'static>>,
}

impl Template {
    /// `None` unless chrono can write `template` for a UTC time. chrono's
    /// strftime parser takes a few specifiers it can only read (`%#z`), so
    /// parsing is not enough: writing one fails, and would fail every
    /// render. Whether a UTC time writes does not depend on which time it
    /// is, so one trial render covers every one.
    pub fn parse(template: &str) -> Option<Self> {
        let items = StrftimeItems::new(template).parse_to_owned().ok()?;
        let parsed = Self { items };
        let mut trial = String::new();
        write!(
            trial,
            "{}",
            Utc::now().format_with_items(parsed.items.iter())
        )
        .ok()?;
        Some(parsed)
    }

    /// Whether it renders differently for different times: text alone
    /// (`logs/cloudtrail`, `100%%`, empty) does not, so a sink with such a
    /// template reads no record's time.
    pub fn timed(&self) -> bool {
        !self.items.iter().all(|i| {
            matches!(
                i,
                Item::Literal(_) | Item::OwnedLiteral(_) | Item::Space(_) | Item::OwnedSpace(_)
            )
        })
    }

    /// Appends the template rendered for `at` to `out`.
    pub fn render_into(&self, at: DateTime<Utc>, out: &mut String) {
        // `parse` made sure every time writes
        let _ = write!(out, "{}", at.format_with_items(self.items.iter()));
    }

    pub fn render(&self, at: DateTime<Utc>) -> Arc<str> {
        let mut out = String::new();
        self.render_into(at, &mut out);
        Arc::from(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_template_of_text_alone_is_not_timed() {
        for timeless in ["", "logs/cloudtrail", "100%%", "a b/%%/%n%t"] {
            assert!(!Template::parse(timeless).unwrap().timed(), "{timeless}");
        }
        for timed in ["%Y", "logs/%F/", "logs/%H", "%s"] {
            assert!(Template::parse(timed).unwrap().timed(), "{timed}");
        }
    }
}
