mod codec;
pub mod envelope;
pub mod error;
pub mod explain;
pub mod plan;
pub mod planner;
pub mod reconstruct;
pub mod stream;
pub mod stream_index;

pub use envelope::{DecodeLimits, ValueEnvelope};
pub use error::{PiLsmError, Result};
pub use explain::{
    explain_envelope, ExplainValue, PhilosophicalCompressionRatio, Purity, StorageClass,
};
pub use plan::{
    ChunkRef, ChunkTransform, LogicalHash, LogicalHashKind, PlanCodec, PlanStream,
    ReconstructionPlan, StreamFingerprint, StreamId,
};
pub use planner::{PlanOptions, Planner};
pub use reconstruct::Reconstructor;
pub use stream::{
    e_hex_fraction_prefix_stream, pi_hex_fraction_prefix_stream, sqrt2_hex_fraction_prefix_stream,
    ByteStream, PrefixByteStream, Sha256CounterStream, StreamRegistry, E_HEX_FRACTION_PREFIX_BYTES,
    PI_HEX_FRACTION_PREFIX_BYTES, SQRT2_HEX_FRACTION_PREFIX_BYTES,
};
pub use stream_index::{IndexedChunk, StreamIndex, StreamIndexOptions};
