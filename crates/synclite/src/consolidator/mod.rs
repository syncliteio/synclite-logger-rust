//! Consolidator facade for the top-level synclite crate.
//!
//! The implementation lives in the extracted consolidator workspace crates,
//! while this module keeps the public API stable for logger callers.

pub use consolidator_core::{
	parse_triggers_file,
	ConsolidatorLayout,
	DestinationSyncMode as DstSyncMode,
	DstDataTypeMapping,
	DstDeviceSchemaNamePolicy,
	DstIdempotentDataIngestionMethod,
	DstObjectInitMode,
	DstType,
	FilterMapperRules,
	MetadataStore,
	ValueMapperRules,
};
pub use consolidator_runtime::Consolidator;


