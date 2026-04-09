use serde_json::{Map, Value};
use std::collections::HashMap;

use super::filter::matches_filter;

/// Execute an aggregation pipeline on a set of documents.
///
/// Supported stages:
///   $match   - filter documents
///   $group   - group by field with accumulators ($sum, $avg, $min, $max, $count, $push, $first, $last)
///   $sort    - sort results
///   $limit   - limit number of results
///   $skip    - skip N results
///   $project - include/exclude fields
///   $count   - count documents into a named field
///   $unwind  - expand array field into multiple docs
///   $lookup  - cross-collection join
pub fn execute_pipeline(
    docs: Vec<Value>,
    pipeline: &[Value],
    collection_resolver: &dyn Fn(&str) -> Vec<Value>,
) -> Result<Vec<Value>, String> {
    let no_index = |_: &str, _: &str, _: &Value| -> Option<Vec<String>> { None };
    execute_pipeline_with_indexes(docs, pipeline, collection_resolver, &no_index)
}

/// Execute a pipeline with optional index-accelerated $lookup.
///
/// `index_lookup_fn(foreign_col, field, value) -> Option<Vec<doc_id>>` returns
/// matching doc IDs from an index if one exists on the foreign field.
pub fn execute_pipeline_with_indexes(
    docs: Vec<Value>,
    pipeline: &[Value],
    collection_resolver: &dyn Fn(&str) -> Vec<Value>,
    index_lookup_fn: &dyn Fn(&str, &str, &Value) -> Option<Vec<String>>,
) -> Result<Vec<Value>, String> {
    let mut result = docs;

    for (i, stage) in pipeline.iter().enumerate() {
        let obj = stage
            .as_object()
            .ok_or_else(|| format!("Pipeline stage {} must be an object", i))?;
        if obj.len() != 1 {
            return Err(format!("Pipeline stage {} must have exactly one key", i));
        }

        let (name, arg) = obj.iter().next().unwrap();
        result = match name.as_str() {
            "$match" => stage_match(result, arg)?,
            "$group" => stage_group(result, arg)?,
            "$sort" => stage_sort(result, arg)?,
            "$limit" => stage_limit(result, arg)?,
            "$skip" => stage_skip(result, arg)?,
            "$project" => stage_project(result, arg)?,
            "$count" => stage_count(result, arg)?,
            "$unwind" => stage_unwind(result, arg)?,
            "$lookup" => stage_lookup(result, arg, collection_resolver, index_lookup_fn)?,
            _ => return Err(format!("Unknown pipeline stage: {}", name)),
        };
    }

    Ok(result)
}

fn stage_match(docs: Vec<Value>, filter: &Value) -> Result<Vec<Value>, String> {
    Ok(docs
        .into_iter()
        .filter(|doc| matches_filter(doc, filter))
        .collect())
}

fn stage_group(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>, String> {
    let obj = spec.as_object().ok_or("$group spec must be an object")?;
    let group_key = obj.get("_id").ok_or("$group requires _id field")?;

    let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
    for doc in docs {
        let key = resolve_group_key(&doc, group_key);
        let key_str = serde_json::to_string(&key).unwrap_or_default();
        groups.entry(key_str).or_default().push(doc);
    }

    let mut results = Vec::with_capacity(groups.len());
    for (key_str, group_docs) in groups {
        let mut result_doc = Map::new();
        let key_val: Value = serde_json::from_str(&key_str).unwrap_or(Value::Null);
        result_doc.insert("_id".to_string(), key_val);

        for (field, acc_spec) in obj {
            if field == "_id" {
                continue;
            }
            let refs: Vec<&Value> = group_docs.iter().collect();
            let value = compute_accumulator(&refs, acc_spec)?;
            result_doc.insert(field.clone(), value);
        }
        results.push(Value::Object(result_doc));
    }

    Ok(results)
}

fn resolve_group_key(doc: &Value, key_spec: &Value) -> Value {
    match key_spec {
        Value::String(s) if s.starts_with('$') => {
            resolve_field_value(doc, &s[1..]).unwrap_or(Value::Null)
        }
        Value::Object(obj) => {
            let mut result = Map::new();
            for (k, v) in obj {
                result.insert(k.clone(), resolve_group_key(doc, v));
            }
            Value::Object(result)
        }
        other => other.clone(),
    }
}

fn compute_accumulator(docs: &[&Value], spec: &Value) -> Result<Value, String> {
    let obj = spec
        .as_object()
        .ok_or("Accumulator must be an object like {\"$sum\": \"$field\"}")?;
    if obj.len() != 1 {
        return Err("Accumulator must have exactly one operator".into());
    }

    let (op, field_spec) = obj.iter().next().unwrap();
    match op.as_str() {
        "$sum" => {
            if let Some(n) = field_spec.as_f64() {
                Ok(Value::from(n * docs.len() as f64))
            } else if let Some(field) = field_spec.as_str().and_then(|s| s.strip_prefix('$')) {
                let sum: f64 = docs
                    .iter()
                    .filter_map(|d| resolve_field_value(d, field))
                    .filter_map(|v| v.as_f64())
                    .sum();
                Ok(Value::from(sum))
            } else {
                Ok(Value::from(0))
            }
        }
        "$avg" => {
            if let Some(field) = field_spec.as_str().and_then(|s| s.strip_prefix('$')) {
                let vals: Vec<f64> = docs
                    .iter()
                    .filter_map(|d| resolve_field_value(d, field))
                    .filter_map(|v| v.as_f64())
                    .collect();
                if vals.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::from(vals.iter().sum::<f64>() / vals.len() as f64))
                }
            } else {
                Ok(Value::Null)
            }
        }
        "$min" => {
            if let Some(field) = field_spec.as_str().and_then(|s| s.strip_prefix('$')) {
                let min = docs
                    .iter()
                    .filter_map(|d| resolve_field_value(d, field))
                    .filter_map(|v| v.as_f64())
                    .fold(f64::INFINITY, f64::min);
                if min.is_infinite() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::from(min))
                }
            } else {
                Ok(Value::Null)
            }
        }
        "$max" => {
            if let Some(field) = field_spec.as_str().and_then(|s| s.strip_prefix('$')) {
                let max = docs
                    .iter()
                    .filter_map(|d| resolve_field_value(d, field))
                    .filter_map(|v| v.as_f64())
                    .fold(f64::NEG_INFINITY, f64::max);
                if max.is_infinite() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::from(max))
                }
            } else {
                Ok(Value::Null)
            }
        }
        "$count" => Ok(Value::from(docs.len() as u64)),
        "$push" => {
            if let Some(field) = field_spec.as_str().and_then(|s| s.strip_prefix('$')) {
                let vals: Vec<Value> = docs
                    .iter()
                    .filter_map(|d| resolve_field_value(d, field))
                    .collect();
                Ok(Value::Array(vals))
            } else {
                Ok(Value::Array(vec![]))
            }
        }
        "$first" => {
            if let Some(field) = field_spec.as_str().and_then(|s| s.strip_prefix('$')) {
                Ok(docs
                    .first()
                    .and_then(|d| resolve_field_value(d, field))
                    .unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        "$last" => {
            if let Some(field) = field_spec.as_str().and_then(|s| s.strip_prefix('$')) {
                Ok(docs
                    .last()
                    .and_then(|d| resolve_field_value(d, field))
                    .unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(format!("Unknown accumulator: {}", op)),
    }
}

fn resolve_field_value(doc: &Value, path: &str) -> Option<Value> {
    let mut current = doc;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(current.clone())
}

fn stage_sort(mut docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>, String> {
    let obj = spec.as_object().ok_or("$sort spec must be an object")?;
    let sort_fields: Vec<(String, bool)> = obj
        .iter()
        .map(|(k, v)| (k.clone(), v.as_i64().unwrap_or(1) < 0))
        .collect();

    docs.sort_by(|a, b| {
        for (field, desc) in &sort_fields {
            let va = a.get(field);
            let vb = b.get(field);
            let ord = compare_values(va, vb);
            if ord != std::cmp::Ordering::Equal {
                return if *desc { ord.reverse() } else { ord };
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(docs)
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(va), Some(vb)) => {
            if let (Some(na), Some(nb)) = (va.as_f64(), vb.as_f64()) {
                return na.partial_cmp(&nb).unwrap_or(Ordering::Equal);
            }
            if let (Some(sa), Some(sb)) = (va.as_str(), vb.as_str()) {
                return sa.cmp(sb);
            }
            va.to_string().cmp(&vb.to_string())
        }
    }
}

fn stage_limit(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>, String> {
    let n = spec
        .as_u64()
        .ok_or("$limit must be a positive integer")? as usize;
    Ok(docs.into_iter().take(n).collect())
}

fn stage_skip(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>, String> {
    let n = spec
        .as_u64()
        .ok_or("$skip must be a positive integer")? as usize;
    Ok(docs.into_iter().skip(n).collect())
}

fn stage_project(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>, String> {
    let obj = spec
        .as_object()
        .ok_or("$project spec must be an object")?;

    let includes: Vec<&str> = obj
        .iter()
        .filter(|(_, v)| v.as_i64().unwrap_or(0) == 1 || v.as_bool().unwrap_or(false))
        .map(|(k, _)| k.as_str())
        .collect();

    let excludes: Vec<&str> = obj
        .iter()
        .filter(|(_, v)| v.as_i64().unwrap_or(1) == 0 || v.as_bool().map_or(false, |b| !b))
        .map(|(k, _)| k.as_str())
        .collect();

    Ok(docs
        .into_iter()
        .map(|doc| {
            if let Some(doc_obj) = doc.as_object() {
                let mut new_obj = Map::new();
                if !includes.is_empty() {
                    if let Some(id) = doc_obj.get("_id") {
                        new_obj.insert("_id".to_string(), id.clone());
                    }
                    for field in &includes {
                        if let Some(v) = doc_obj.get(*field) {
                            new_obj.insert(field.to_string(), v.clone());
                        }
                    }
                } else if !excludes.is_empty() {
                    for (k, v) in doc_obj {
                        if !excludes.contains(&k.as_str()) {
                            new_obj.insert(k.clone(), v.clone());
                        }
                    }
                } else {
                    return doc;
                }
                Value::Object(new_obj)
            } else {
                doc
            }
        })
        .collect())
}

fn stage_count(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>, String> {
    let field = spec
        .as_str()
        .ok_or("$count must be a string (the output field name)")?;
    let mut result = Map::new();
    result.insert(field.to_string(), Value::from(docs.len() as u64));
    Ok(vec![Value::Object(result)])
}

fn stage_unwind(docs: Vec<Value>, spec: &Value) -> Result<Vec<Value>, String> {
    let field = match spec {
        Value::String(s) => s.strip_prefix('$').unwrap_or(s).to_string(),
        Value::Object(obj) => obj
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.strip_prefix('$').unwrap_or(s).to_string())
            .ok_or("$unwind object requires 'path' field")?,
        _ => return Err("$unwind requires a string or object".into()),
    };

    let mut results = Vec::new();
    for doc in docs {
        if let Some(arr) = doc.get(&field).and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(mut obj) = doc.as_object().cloned() {
                    obj.insert(field.clone(), item.clone());
                    results.push(Value::Object(obj));
                }
            }
        } else {
            results.push(doc);
        }
    }
    Ok(results)
}

fn stage_lookup(
    docs: Vec<Value>,
    spec: &Value,
    resolver: &dyn Fn(&str) -> Vec<Value>,
    index_lookup_fn: &dyn Fn(&str, &str, &Value) -> Option<Vec<String>>,
) -> Result<Vec<Value>, String> {
    let obj = spec.as_object().ok_or("$lookup spec must be an object")?;

    let from = obj
        .get("from")
        .and_then(|v| v.as_str())
        .ok_or("$lookup requires 'from'")?;
    let local_field = obj
        .get("localField")
        .and_then(|v| v.as_str())
        .ok_or("$lookup requires 'localField'")?;
    let foreign_field = obj
        .get("foreignField")
        .and_then(|v| v.as_str())
        .ok_or("$lookup requires 'foreignField'")?;
    let as_field = obj
        .get("as")
        .and_then(|v| v.as_str())
        .ok_or("$lookup requires 'as'")?;

    // Check if an index exists on the foreign field by probing with a dummy value.
    // If index_lookup_fn returns Some for any value, we use indexed lookups.
    let use_index = index_lookup_fn(from, foreign_field, &Value::Null).is_some();

    if use_index {
        // Index-accelerated $lookup: O(n * log(m)) instead of O(n * m) (#19)
        let foreign_docs = resolver(from);
        // Build a quick id->doc map for resolving index hits
        let foreign_by_id: std::collections::HashMap<&str, &Value> = foreign_docs
            .iter()
            .filter_map(|d| {
                let id = d.get("_id")?.as_str()?;
                Some((id, d))
            })
            .collect();

        Ok(docs
            .into_iter()
            .map(|doc| {
                let matches = if let Some(local_val) = doc.get(local_field) {
                    if let Some(ids) = index_lookup_fn(from, foreign_field, local_val) {
                        ids.iter()
                            .filter_map(|id| foreign_by_id.get(id.as_str()).cloned())
                            .cloned()
                            .collect()
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };
                if let Some(mut obj) = doc.as_object().cloned() {
                    obj.insert(as_field.to_string(), Value::Array(matches));
                    Value::Object(obj)
                } else {
                    doc
                }
            })
            .collect())
    } else {
        // Fallback: O(n * m) nested loop join
        let foreign_docs = resolver(from);

        Ok(docs
            .into_iter()
            .map(|doc| {
                let local_val = doc.get(local_field);
                let matches: Vec<Value> = foreign_docs
                    .iter()
                    .filter(|fd| fd.get(foreign_field) == local_val)
                    .cloned()
                    .collect();
                if let Some(mut obj) = doc.as_object().cloned() {
                    obj.insert(as_field.to_string(), Value::Array(matches));
                    Value::Object(obj)
                } else {
                    doc
                }
            })
            .collect())
    }
}
