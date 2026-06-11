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
pub use explain::{explain_envelope, ExplainValue, Purity, StorageClass};
pub use plan::{
    ChunkRef, ChunkTransform, LogicalHash, LogicalHashKind, PlanCodec, PlanStream,
    ReconstructionPlan, StreamFingerprint, StreamId,
};
pub use planner::{PlanOptions, Planner};
pub use reconstruct::Reconstructor;
pub use stream::{
    pi_hex_fraction_prefix_stream, ByteStream, PrefixByteStream, Sha256CounterStream,
    StreamRegistry, PI_HEX_FRACTION_PREFIX_BYTES,
};
pub use stream_index::{IndexedChunk, StreamIndex, StreamIndexOptions};
