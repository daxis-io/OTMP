//! `DataFusion` 55 table-provider integration for authenticated OTMP metadata readers.

mod footer;
mod footer_bounds;
mod provider;
mod pruning;
pub mod store;

pub use provider::{OtmpTableProvider, ProviderOptions, ProviderStatistics, schema_to_arrow};

mod schemaadapter;
