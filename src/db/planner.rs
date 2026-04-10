use serde_json::Value;
use std::sync::Arc;

use super::index::Index;

// ---- Predicate extraction ----------------------------------

/// A single predicate extracted from a filter expression.
#[derive(Debug, Clone)]
pub struct Predicate {
    pub field: String,
    pub op: String,
    pub value: Value,
}

/// A query execution plan.
pub enum QueryPlan {
    /// No index available — scan all documents.
    FullScan,
    /// Use an index to get candidate doc IDs, then apply residual filter.
    IndexScan {
        index: Arc<dyn Index>,
        predicate: Predicate,
        /// Remaining filter predicates not covered by the index scan.
        /// None means the index fully satisfies the filter.
        residual_filter: Option<Value>,
    },
}

/// Analyze a filter and select the best index to use.
///
/// Strategy: extract top-level field predicates, match against available
/// indexes, pick the most selective one (equality > range > set).
pub fn plan_query(filter: &Value, available_indexes: &[Arc<dyn Index>]) -> QueryPlan {
    if available_indexes.is_empty() {
        return QueryPlan::FullScan;
    }

    let predicates = extract_predicates(filter);
    if predicates.is_empty() {
        return QueryPlan::FullScan;
    }

    let mut best: Option<(Predicate, Arc<dyn Index>, u32)> = None;

    for pred in &predicates {
        for idx in available_indexes {
            if idx.field() == pred.field && idx.supports_op(&pred.op) {
                let selectivity = estimate_selectivity(pred);
                let is_better = match &best {
                    None => true,
                    Some((_, _, current_best)) => selectivity < *current_best,
                };
                if is_better {
                    best = Some((pred.clone(), Arc::clone(idx), selectivity));
                }
            }
        }
    }

    match best {
        None => QueryPlan::FullScan,
        Some((predicate, index, _)) => {
            let residual = build_residual_filter(filter, &predicate);
            QueryPlan::IndexScan {
                index,
                predicate,
                residual_filter: residual,
            }
        }
    }
}

/// Explanation of a query plan for EXPLAIN output.
#[derive(Debug, Clone)]
pub struct QueryExplanation {
    pub plan_type: String,
    pub index_field: Option<String>,
    pub index_type: Option<String>,
    pub predicate: Option<String>,
    pub has_residual: bool,
    pub selectivity: f64,
    pub total_docs: usize,
    pub estimated_scan: usize,
}

/// Explain a query without executing it.
pub fn explain_query(
    filter: &Value,
    available_indexes: &[Arc<dyn Index>],
    total_docs: usize,
) -> QueryExplanation {
    if available_indexes.is_empty() {
        return QueryExplanation {
            plan_type: "FullScan".to_string(),
            index_field: None,
            index_type: None,
            predicate: None,
            has_residual: false,
            selectivity: 1.0,
            total_docs,
            estimated_scan: total_docs,
        };
    }

    let predicates = extract_predicates(filter);
    if predicates.is_empty() {
        return QueryExplanation {
            plan_type: "FullScan".to_string(),
            index_field: None,
            index_type: None,
            predicate: None,
            has_residual: false,
            selectivity: 1.0,
            total_docs,
            estimated_scan: total_docs,
        };
    }

    let mut best: Option<(Predicate, Arc<dyn Index>, u32)> = None;

    for pred in &predicates {
        for idx in available_indexes {
            if idx.field() == pred.field && idx.supports_op(&pred.op) {
                let sel = estimate_selectivity(pred);
                let is_better = match &best {
                    None => true,
                    Some((_, _, current)) => sel < *current,
                };
                if is_better {
                    best = Some((pred.clone(), Arc::clone(idx), sel));
                }
            }
        }
    }

    match best {
        None => QueryExplanation {
            plan_type: "FullScan".to_string(),
            index_field: None,
            index_type: None,
            predicate: None,
            has_residual: false,
            selectivity: 1.0,
            total_docs,
            estimated_scan: total_docs,
        },
        Some((pred, idx, sel_score)) => {
            let residual = build_residual_filter(filter, &pred);
            let selectivity = match sel_score {
                1 => 0.05,   // eq: ~5%
                2 => 0.15,   // in: ~15%
                5 => 0.33,   // range: ~33%
                _ => 0.5,
            };
            let estimated = (total_docs as f64 * selectivity).ceil() as usize;
            QueryExplanation {
                plan_type: "IndexScan".to_string(),
                index_field: Some(idx.field().to_string()),
                index_type: Some(format!("{:?}", idx.index_type())),
                predicate: Some(format!("{} {} {}", pred.field, pred.op, pred.value)),
                has_residual: residual.is_some(),
                selectivity,
                total_docs,
                estimated_scan: estimated.max(1),
            }
        }
    }
}

/// Execute an index scan for a predicate, returning candidate document IDs.
pub fn execute_index_scan(index: &dyn Index, predicate: &Predicate) -> Vec<String> {
    match predicate.op.as_str() {
        "$eq" => index.lookup_eq(&predicate.value),
        "$in" => {
            if let Some(arr) = predicate.value.as_array() {
                let mut ids = Vec::new();
                for val in arr {
                    ids.extend(index.lookup_eq(val));
                }
                ids.sort();
                ids.dedup();
                ids
            } else {
                Vec::new()
            }
        }
        "$gt" => index.lookup_range(Some(&predicate.value), None, false, false),
        "$gte" => index.lookup_range(Some(&predicate.value), None, true, false),
        "$lt" => index.lookup_range(None, Some(&predicate.value), false, false),
        "$lte" => index.lookup_range(None, Some(&predicate.value), false, true),
        _ => Vec::new(),
    }
}

/// Extract flat predicates from a filter object.
///
/// Only extracts top-level field conditions (not nested $and/$or).
/// For compound filters like `$and`, we still extract individual predicates
/// from each sub-filter to find indexable fields.
fn extract_predicates(filter: &Value) -> Vec<Predicate> {
    let obj = match filter.as_object() {
        Some(o) => o,
        None => return Vec::new(),
    };

    let mut predicates = Vec::new();

    for (key, condition) in obj {
        match key.as_str() {
            "$and" => {
                if let Some(arr) = condition.as_array() {
                    for sub in arr {
                        predicates.extend(extract_predicates(sub));
                    }
                }
            }
            "$or" | "$not" => {
                // Can't use indexes efficiently for $or/$not at top level
            }
            field => {
                if let Some(cond_obj) = condition.as_object() {
                    let has_ops = cond_obj.keys().any(|k| k.starts_with('$'));
                    if has_ops {
                        for (op, val) in cond_obj {
                            predicates.push(Predicate {
                                field: field.to_string(),
                                op: op.clone(),
                                value: val.clone(),
                            });
                        }
                    } else {
                        // Implicit $eq with an object value
                        predicates.push(Predicate {
                            field: field.to_string(),
                            op: "$eq".to_string(),
                            value: condition.clone(),
                        });
                    }
                } else {
                    // Implicit $eq
                    predicates.push(Predicate {
                        field: field.to_string(),
                        op: "$eq".to_string(),
                        value: condition.clone(),
                    });
                }
            }
        }
    }

    predicates
}

/// Estimate selectivity of a predicate (lower = more selective = better).
fn estimate_selectivity(pred: &Predicate) -> u32 {
    match pred.op.as_str() {
        "$eq" => 1,   // Most selective
        "$in" => 2,   // Depends on array size, but generally good
        "$gt" | "$gte" | "$lt" | "$lte" => 5, // Range — less selective
        "$nin" => 8,  // Inverted — least useful for index
        _ => 10,
    }
}

/// Build a residual filter by removing the indexed predicate from the original filter.
///
/// If the original filter has only one predicate (the indexed one), returns None.
fn build_residual_filter(filter: &Value, indexed_pred: &Predicate) -> Option<Value> {
    let obj = filter.as_object()?;

    // Simple case: filter is just {"field": value} or {"field": {"$op": value}}
    if obj.len() == 1 {
        let (key, condition) = obj.iter().next().unwrap();
        if key == &indexed_pred.field {
            if let Some(cond_obj) = condition.as_object() {
                if cond_obj.len() == 1 && cond_obj.contains_key(&indexed_pred.op) {
                    return None; // Fully covered
                }
                // Multiple operators on the same field — remove the indexed one
                let remaining: serde_json::Map<String, Value> = cond_obj
                    .iter()
                    .filter(|(op, _)| *op != &indexed_pred.op)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if remaining.is_empty() {
                    return None;
                }
                let mut result = serde_json::Map::new();
                result.insert(key.clone(), Value::Object(remaining));
                return Some(Value::Object(result));
            } else {
                // Simple equality covered by index
                return None;
            }
        }
    }

    // Multi-field filter: remove the indexed field's predicate
    let mut remaining = serde_json::Map::new();
    for (key, condition) in obj {
        if key == &indexed_pred.field {
            if let Some(cond_obj) = condition.as_object() {
                let has_ops = cond_obj.keys().any(|k| k.starts_with('$'));
                if has_ops {
                    let filtered: serde_json::Map<String, Value> = cond_obj
                        .iter()
                        .filter(|(op, _)| *op != &indexed_pred.op)
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    if !filtered.is_empty() {
                        remaining.insert(key.clone(), Value::Object(filtered));
                    }
                }
                // If no operators left for this field, skip it entirely
            }
            // Simple equality — skip, it's covered
        } else {
            remaining.insert(key.clone(), condition.clone());
        }
    }

    if remaining.is_empty() {
        None
    } else {
        Some(Value::Object(remaining))
    }
}

// ---- Tests ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::index::{BTreeIndex, HashIndex, IndexDef, IndexType};
    use serde_json::json;

    fn make_btree_index(field: &str) -> Arc<dyn Index> {
        let def = IndexDef {
            collection: "test/users".into(),
            field: field.to_string(),
            unique: false,
            index_type: IndexType::BTree,
        };
        let idx = Arc::new(BTreeIndex::new(def));
        let docs = vec![
            ("1".into(), json!({"name": "Alice", "age": 30, "city": "NYC"})),
            ("2".into(), json!({"name": "Bob", "age": 25, "city": "LA"})),
            ("3".into(), json!({"name": "Charlie", "age": 35, "city": "NYC"})),
        ];
        idx.build_from(&docs);
        idx as Arc<dyn Index>
    }

    fn make_hash_index(field: &str) -> Arc<dyn Index> {
        let def = IndexDef {
            collection: "test/users".into(),
            field: field.to_string(),
            unique: false,
            index_type: IndexType::Hash,
        };
        let idx = Arc::new(HashIndex::new(def));
        let docs = vec![
            ("1".into(), json!({"name": "Alice", "age": 30, "city": "NYC"})),
            ("2".into(), json!({"name": "Bob", "age": 25, "city": "LA"})),
            ("3".into(), json!({"name": "Charlie", "age": 35, "city": "NYC"})),
        ];
        idx.build_from(&docs);
        idx as Arc<dyn Index>
    }

    #[test]
    fn test_plan_eq_uses_index() {
        let indexes = vec![make_btree_index("name")];
        let filter = json!({"name": "Alice"});
        let plan = plan_query(&filter, &indexes);

        match plan {
            QueryPlan::IndexScan {
                predicate,
                residual_filter,
                ..
            } => {
                assert_eq!(predicate.field, "name");
                assert_eq!(predicate.op, "$eq");
                assert!(residual_filter.is_none());
            }
            QueryPlan::FullScan => panic!("Expected IndexScan"),
        }
    }

    #[test]
    fn test_plan_no_matching_index_falls_back() {
        let indexes = vec![make_btree_index("name")];
        let filter = json!({"age": {"$gt": 25}});
        let plan = plan_query(&filter, &indexes);

        assert!(matches!(plan, QueryPlan::FullScan));
    }

    #[test]
    fn test_plan_multi_field_with_residual() {
        let indexes = vec![make_btree_index("name")];
        let filter = json!({"name": "Alice", "age": {"$gt": 25}});
        let plan = plan_query(&filter, &indexes);

        match plan {
            QueryPlan::IndexScan {
                predicate,
                residual_filter,
                ..
            } => {
                assert_eq!(predicate.field, "name");
                let residual = residual_filter.expect("Should have residual");
                assert!(residual.get("age").is_some());
                assert!(residual.get("name").is_none());
            }
            QueryPlan::FullScan => panic!("Expected IndexScan"),
        }
    }

    #[test]
    fn test_plan_prefers_eq_over_range() {
        let name_idx = make_btree_index("name");
        let age_idx = make_btree_index("age");
        let indexes = vec![name_idx, age_idx];
        let filter = json!({"name": "Alice", "age": {"$gt": 25}});
        let plan = plan_query(&filter, &indexes);

        match plan {
            QueryPlan::IndexScan { predicate, .. } => {
                // Should prefer $eq on name over $gt on age
                assert_eq!(predicate.field, "name");
                assert_eq!(predicate.op, "$eq");
            }
            QueryPlan::FullScan => panic!("Expected IndexScan"),
        }
    }

    #[test]
    fn test_execute_index_scan_eq() {
        let idx = make_btree_index("city");
        let pred = Predicate {
            field: "city".to_string(),
            op: "$eq".to_string(),
            value: json!("NYC"),
        };
        let ids = execute_index_scan(idx.as_ref(), &pred);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"1".to_string()));
        assert!(ids.contains(&"3".to_string()));
    }

    #[test]
    fn test_execute_index_scan_in() {
        let idx = make_btree_index("name");
        let pred = Predicate {
            field: "name".to_string(),
            op: "$in".to_string(),
            value: json!(["Alice", "Bob"]),
        };
        let ids = execute_index_scan(idx.as_ref(), &pred);
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn test_execute_index_scan_range() {
        let idx = make_btree_index("age");
        let pred = Predicate {
            field: "age".to_string(),
            op: "$gt".to_string(),
            value: json!(28),
        };
        let ids = execute_index_scan(idx.as_ref(), &pred);
        // ages 30, 35 — both > 28
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn test_hash_index_used_for_eq() {
        let indexes = vec![make_hash_index("name")];
        let filter = json!({"name": "Alice"});
        let plan = plan_query(&filter, &indexes);

        match plan {
            QueryPlan::IndexScan { predicate, .. } => {
                assert_eq!(predicate.field, "name");
            }
            QueryPlan::FullScan => panic!("Expected IndexScan"),
        }
    }

    #[test]
    fn test_hash_index_not_used_for_range() {
        let indexes = vec![make_hash_index("age")];
        let filter = json!({"age": {"$gt": 25}});
        let plan = plan_query(&filter, &indexes);

        // Hash doesn't support $gt
        assert!(matches!(plan, QueryPlan::FullScan));
    }

    #[test]
    fn test_plan_empty_filter() {
        let indexes = vec![make_btree_index("name")];
        let filter = json!({});
        let plan = plan_query(&filter, &indexes);
        assert!(matches!(plan, QueryPlan::FullScan));
    }

    #[test]
    fn test_extract_predicates_from_and() {
        let filter = json!({"$and": [{"name": "Alice"}, {"age": {"$gt": 25}}]});
        let preds = extract_predicates(&filter);
        assert_eq!(preds.len(), 2);
        assert!(preds.iter().any(|p| p.field == "name" && p.op == "$eq"));
        assert!(preds.iter().any(|p| p.field == "age" && p.op == "$gt"));
    }

    #[test]
    fn test_explain_with_index() {
        let indexes = vec![make_btree_index("age")];
        let filter = json!({"age": {"$gt": 25}});
        let explanation = explain_query(&filter, &indexes, 1000);
        assert_eq!(explanation.plan_type, "IndexScan");
        assert_eq!(explanation.index_field.as_deref(), Some("age"));
        assert!(explanation.selectivity < 1.0);
        assert!(explanation.estimated_scan < 1000);
    }

    #[test]
    fn test_explain_full_scan() {
        let filter = json!({"name": "Alice"});
        let explanation = explain_query(&filter, &[], 500);
        assert_eq!(explanation.plan_type, "FullScan");
        assert_eq!(explanation.index_field, None);
        assert_eq!(explanation.selectivity, 1.0);
        assert_eq!(explanation.estimated_scan, 500);
    }
}
