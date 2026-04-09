pub mod aggregation;
pub mod backup;
pub mod changefeed;
pub mod database;
pub mod error;
pub mod filter;
pub mod index;
pub mod planner;
pub mod schema;
pub mod transaction;

pub use changefeed::ChangeFeed;
pub use database::{DatabaseManager, QueryOptions};
pub use error::VantaError;
pub use filter::matches_filter;
pub use index::{IndexDef, IndexType};
pub use schema::CollectionSchema;
pub use transaction::{TransactionManager, TransactionOp};
