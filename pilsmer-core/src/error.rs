use thiserror::Error;

pub type Result<T> = std::result::Result<T, PiLsmError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PiLsmError {
    #[error("invalid PLSM magic")]
    InvalidMagic,
    #[error("unsupported envelope version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown envelope kind {0}")]
    UnknownEnvelopeKind(u8),
    #[error("unknown logical hash kind {0}")]
    UnknownLogicalHashKind(u8),
    #[error("unknown plan codec {0}")]
    UnknownPlanCodec(u8),
    #[error("unknown chunk kind {0}")]
    UnknownChunkKind(u8),
    #[error("unknown chunk transform {0}")]
    UnknownChunkTransform(u8),
    #[error("frame crc mismatch")]
    FrameCrcMismatch,
    #[error("logical hash mismatch")]
    LogicalHashMismatch,
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("varint is too large")]
    VarintOverflow,
    #[error("decode limit exceeded: {0}")]
    DecodeLimitExceeded(&'static str),
    #[error("invalid plan: {0}")]
    InvalidPlan(&'static str),
    #[error("missing stream")]
    MissingStream,
    #[error("stream fingerprint mismatch")]
    StreamFingerprintMismatch,
    #[error("stream read is out of bounds")]
    StreamReadOutOfBounds,
    #[error("planning failed: {0}")]
    PlanningFailed(&'static str),
    #[error("index limit exceeded: {0}")]
    IndexLimitExceeded(&'static str),
    #[error("arithmetic overflow")]
    ArithmeticOverflow,
}
