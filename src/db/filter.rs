use std::cell::RefCell;
use std::collections::HashMap;

use serde_json::Value;

/// Thread-local regex cache to avoid recompiling patterns on every filter evaluation.
/// Simple LRU-style eviction: clear all when capacity exceeded.
const REGEX_CACHE_CAPACITY: usize = 64;

thread_local! {
    static REGEX_CACHE: RefCell<HashMap<String, regex::Regex>> = RefCell::new(HashMap::new());
}

pub fn cached_regex(pattern: &str) -> Option<regex::Regex> {
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(pattern) {
            return Some(re.clone());
        }
        if let Ok(re) = regex::Regex::new(pattern) {
            if cache.len() >= REGEX_CACHE_CAPACITY {
                cache.clear();
            }
            cache.insert(pattern.to_string(), re.clone());
            Some(re)
        } else {
            None
        }
    })
}

/// Evaluate whether a document matches a filter expression.
///
/// Filter syntax (MongoDB-compatible):
///   {"field": "value"}                     - equality
///   {"field": {"$gt": 10}}                 - comparison
///   {"$and": [{...}, {...}]}               - logical AND
///   {"$or": [{...}, {...}]}                - logical OR
///   {"$not": {...}}                        - logical NOT
///   {"field": {"$in": [1, 2, 3]}}          - set membership
///   {"field": {"$nin": [1, 2, 3]}}         - not in set
///   {"field": {"$exists": true}}           - field exists
///   {"field": {"$regex": "pattern"}}       - regex match
///   {"field": {"$contains": "substr"}}     - substring match
///   {"field": {"$starts_with": "prefix"}}  - prefix match
///   {"field": {"$ends_with": "suffix"}}    - suffix match
///   {"address.city": "NYC"}                - dot-path nested fields
pub fn matches_filter(doc: &Value, filter: &Value) -> bool {
    let obj = match filter.as_object() {
        Some(o) if !o.is_empty() => o,
        Some(_) => return true, // empty filter matches all
        None => return false,
    };

    for (key, condition) in obj {
        match key.as_str() {
            "$and" => {
                if let Some(arr) = condition.as_array() {
                    if !arr.iter().all(|f| matches_filter(doc, f)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            "$or" => {
                if let Some(arr) = condition.as_array() {
                    if !arr.iter().any(|f| matches_filter(doc, f)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            "$not" => {
                if matches_filter(doc, condition) {
                    return false;
                }
            }
            field => {
                let doc_value = resolve_field(doc, field);
                if !matches_condition(doc_value, condition) {
                    return false;
                }
            }
        }
    }
    true
}

/// Resolve a dotted field path (e.g., "address.city") from a document.
pub fn resolve_field<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = doc;
    for part in path.split('.') {
        match current.get(part) {
            Some(v) => current = v,
            None => return None,
        }
    }
    Some(current)
}

/// Check if a document field value matches a condition.
/// Condition can be a plain value (implicit $eq) or an object with operators.
fn matches_condition(doc_value: Option<&Value>, condition: &Value) -> bool {
    if let Some(obj) = condition.as_object() {
        let has_operators = obj.keys().any(|k| k.starts_with('$'));
        if has_operators {
            return obj.iter().all(|(op, operand)| match_operator(doc_value, op, operand));
        }
    }
    // Implicit $eq
    match doc_value {
        Some(v) => v == condition,
        None => condition.is_null(),
    }
}

fn match_operator(doc_value: Option<&Value>, op: &str, operand: &Value) -> bool {
    match op {
        "$eq" => doc_value.map_or(operand.is_null(), |v| v == operand),
        "$ne" => doc_value.map_or(!operand.is_null(), |v| v != operand),
        "$gt" => compare_op(doc_value, operand, |ord| ord == std::cmp::Ordering::Greater),
        "$gte" => compare_op(doc_value, operand, |ord| ord != std::cmp::Ordering::Less),
        "$lt" => compare_op(doc_value, operand, |ord| ord == std::cmp::Ordering::Less),
        "$lte" => compare_op(doc_value, operand, |ord| ord != std::cmp::Ordering::Greater),
        "$in" => {
            if let Some(arr) = operand.as_array() {
                doc_value.map_or(false, |v| arr.contains(v))
            } else {
                false
            }
        }
        "$nin" => {
            if let Some(arr) = operand.as_array() {
                doc_value.map_or(true, |v| !arr.contains(v))
            } else {
                true
            }
        }
        "$exists" => {
            let should_exist = operand.as_bool().unwrap_or(true);
            doc_value.is_some() == should_exist
        }
        "$regex" => {
            if let (Some(val), Some(pattern)) =
                (doc_value.and_then(|v| v.as_str()), operand.as_str())
            {
                cached_regex(pattern)
                    .map(|re| re.is_match(val))
                    .unwrap_or(false)
            } else {
                false
            }
        }
        "$contains" => {
            if let (Some(val), Some(substr)) =
                (doc_value.and_then(|v| v.as_str()), operand.as_str())
            {
                val.contains(substr)
            } else {
                false
            }
        }
        "$starts_with" => {
            if let (Some(val), Some(prefix)) =
                (doc_value.and_then(|v| v.as_str()), operand.as_str())
            {
                val.starts_with(prefix)
            } else {
                false
            }
        }
        "$ends_with" => {
            if let (Some(val), Some(suffix)) =
                (doc_value.and_then(|v| v.as_str()), operand.as_str())
            {
                val.ends_with(suffix)
            } else {
                false
            }
        }
        _ => false,
    }
}

fn compare_op<F>(doc_value: Option<&Value>, operand: &Value, pred: F) -> bool
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    let dv = match doc_value {
        Some(v) => v,
        None => return false,
    };
    if let (Some(a), Some(b)) = (dv.as_f64(), operand.as_f64()) {
        return a.partial_cmp(&b).map_or(false, &pred);
    }
    if let (Some(a), Some(b)) = (dv.as_str(), operand.as_str()) {
        return pred(a.cmp(b));
    }
    if let (Some(a), Some(b)) = (dv.as_bool(), operand.as_bool()) {
        return pred(a.cmp(&b));
    }
    false
}
