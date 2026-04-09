use serde_json::Value;

use super::aggregation;
use super::database::{DatabaseManager, QueryOptions};
use super::error::VantaError;
use super::filter::matches_filter;
use super::planner::{self, QueryPlan};

impl DatabaseManager {
    // ---- Rich query (filter) ---------------------------------

    pub fn query(
        &self,
        db: &str,
        collection: &str,
        filter: &Value,
        opts: &QueryOptions,
    ) -> Result<(Vec<Value>, usize), VantaError> {
        self.require_db(db)?;
        let store = self.db_engine(db)?;

        let idx_key = Self::idx_key(db, collection);
        let indexes = self.index_manager.get_all_indexes(&idx_key);
        let plan = planner::plan_query(filter, &indexes);

        let filtered = match plan {
            QueryPlan::FullScan => {
                let all = self.find_all(db, collection)?;
                all.into_iter()
                    .filter(|doc| matches_filter(doc, filter))
                    .collect()
            }
            QueryPlan::IndexScan {
                index,
                predicate,
                residual_filter,
            } => {
                let candidate_ids = planner::execute_index_scan(index.as_ref(), &predicate);
                let mut docs = Vec::with_capacity(candidate_ids.len());

                for id in &candidate_ids {
                    if let Some(data) = store.get_latest(collection, id) {
                        if let Ok(doc) = serde_json::from_slice::<Value>(&data) {
                            let passes = match &residual_filter {
                                Some(rf) => matches_filter(&doc, rf),
                                None => true,
                            };
                            if passes {
                                docs.push(doc);
                            }
                        }
                    }
                }
                docs
            }
        };

        Ok(opts.apply(filtered))
    }

    // ---- Aggregation -----------------------------------------

    pub fn aggregate(
        &self,
        db: &str,
        collection: &str,
        pipeline: &[Value],
    ) -> Result<Vec<Value>, VantaError> {
        let docs = self.find_all(db, collection)?;

        let db_owned = db.to_string();
        let self_ref = &self;
        let resolver = move |foreign_col: &str| -> Vec<Value> {
            self_ref
                .find_all(&db_owned, foreign_col)
                .unwrap_or_default()
        };

        let db_for_lookup = db.to_string();
        let lookup_index_fn =
            move |foreign_col: &str, field: &str, value: &Value| -> Option<Vec<String>> {
                let idx_key = Self::idx_key(&db_for_lookup, foreign_col);
                let idx = self.index_manager.get_index(&idx_key, field)?;
                Some(idx.lookup_eq(value))
            };

        aggregation::execute_pipeline_with_indexes(
            docs,
            pipeline,
            &resolver,
            &lookup_index_fn,
        )
        .map_err(|e| VantaError::Internal(e))
    }
}
