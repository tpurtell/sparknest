//! Recent log lines kept in memory on every node, so the web UI and
//! `nest logs` can show them without knowing where stderr went (a file
//! under the trial scripts, the journal under systemd). The same filter as
//! the console applies; the oldest lines fall off past `CAPACITY`.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::OnceLock;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;

pub const CAPACITY: usize = 20_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogLine {
    /// Monotonic per node: pass the last one seen as `after` to follow.
    pub seq: u64,
    /// Unix milliseconds.
    pub ts_ms: u64,
    /// ERROR, WARN, INFO, DEBUG or TRACE.
    pub level: String,
    pub target: String,
    /// The message followed by its fields as `key=value`.
    pub message: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LogQuery {
    /// Least severe level to include (default INFO).
    pub level: Option<String>,
    /// Only lines whose target or message contain this (case-insensitive).
    pub contains: Option<String>,
    /// Only lines with a larger `seq`.
    pub after: Option<u64>,
    /// The newest this many (default 500).
    pub limit: Option<usize>,
}

struct Ring {
    next: u64,
    lines: VecDeque<LogLine>,
}

fn ring() -> &'static Mutex<Ring> {
    static RING: OnceLock<Mutex<Ring>> = OnceLock::new();
    RING.get_or_init(|| {
        Mutex::new(Ring {
            next: 1,
            lines: VecDeque::new(),
        })
    })
}

fn rank(level: &str) -> u8 {
    match level.to_ascii_uppercase().as_str() {
        "ERROR" => 0,
        "WARN" => 1,
        "INFO" => 2,
        "DEBUG" => 3,
        _ => 4,
    }
}

/// Matching lines, oldest first.
pub fn recent(q: &LogQuery) -> Vec<LogLine> {
    let max = rank(q.level.as_deref().unwrap_or("INFO"));
    let needle = q.contains.as_ref().map(|s| s.to_lowercase());
    let limit = q.limit.unwrap_or(500).min(CAPACITY);
    let r = ring().lock();
    let mut out: Vec<LogLine> = r
        .lines
        .iter()
        .rev()
        .take_while(|l| q.after.is_none_or(|a| l.seq > a))
        .filter(|l| rank(&l.level) <= max)
        .filter(|l| {
            needle.as_ref().is_none_or(|n| {
                l.message.to_lowercase().contains(n) || l.target.to_lowercase().contains(n)
            })
        })
        .take(limit)
        .cloned()
        .collect();
    out.reverse();
    out
}

/// Record a line (also used by tests).
pub fn push(level: &str, target: &str, message: String) {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut r = ring().lock();
    let seq = r.next;
    r.next += 1;
    if r.lines.len() >= CAPACITY {
        r.lines.pop_front();
    }
    r.lines.push_back(LogLine {
        seq,
        ts_ms,
        level: level.to_string(),
        target: target.to_string(),
        message,
    });
}

#[derive(Default)]
struct Fields {
    message: String,
    rest: String,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            let _ = write!(self.rest, " {}={}", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.rest, " {}={value:?}", field.name());
        }
    }
}

/// The layer that fills the ring; add it next to the console layer.
pub struct RingLayer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RingLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut f = Fields::default();
        event.record(&mut f);
        let meta = event.metadata();
        push(
            meta.level().as_str(),
            meta.target(),
            format!("{}{}", f.message, f.rest),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_by_level_text_and_limit() {
        // The ring is process-wide: tag lines so other tests do not interfere.
        let tag = format!("t{}", rand::random::<u32>());
        push("INFO", "nest_x", format!("{tag} one"));
        push("WARN", "nest_x", format!("{tag} two path=/hub/a"));
        push("DEBUG", "nest_x", format!("{tag} three"));
        push("ERROR", "nest_y", format!("{tag} four"));
        let q = |level: &str, extra: Option<&str>, limit| {
            recent(&LogQuery {
                level: Some(level.into()),
                contains: Some(extra.map_or(tag.clone(), |e| format!("{tag} {e}"))),
                after: None,
                limit,
            })
            .into_iter()
            .map(|l| l.message.split(' ').nth(1).unwrap().to_string())
            .collect::<Vec<_>>()
        };
        assert_eq!(q("info", None, None), ["one", "two", "four"]);
        assert_eq!(q("warn", None, None), ["two", "four"]);
        assert_eq!(
            q("debug", None, Some(2)),
            ["three", "four"],
            "newest, oldest first"
        );
        assert_eq!(q("info", Some("TWO PATH=/HUB"), None), ["two"]);
        let all = recent(&LogQuery {
            contains: Some(tag.clone()),
            ..Default::default()
        });
        let after = recent(&LogQuery {
            contains: Some(tag),
            after: Some(all[0].seq),
            ..Default::default()
        });
        assert_eq!(after.len(), all.len() - 1);
    }
}
