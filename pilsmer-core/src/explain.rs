use crate::envelope::ValueEnvelope;
use crate::plan::ChunkRef;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageClass {
    Raw,
    Plan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purity {
    Pure,
    Contaminated,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PhilosophicalCompressionRatio {
    Finite(f64),
    Infinite,
    Revoked,
    Undefined,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExplainValue {
    pub storage_class: StorageClass,
    pub logical_user_bytes: u64,
    pub physical_value_bytes: u64,
    pub plan_metadata_bytes: u64,
    pub chunks: u64,
    pub longest_natural_run: u32,
    pub literal_bytes: u64,
    pub philosophical_user_value_bytes_stored: u64,
    pub metadata_amplification_ratio: Option<f64>,
    pub average_chunk_len: Option<f64>,
    pub philosophical_compression_ratio: PhilosophicalCompressionRatio,
    pub purity: Purity,
}

pub fn explain_envelope(envelope: &ValueEnvelope, encoded_len: usize) -> ExplainValue {
    match envelope {
        ValueEnvelope::Raw(bytes) => ExplainValue {
            storage_class: StorageClass::Raw,
            logical_user_bytes: bytes.len() as u64,
            physical_value_bytes: encoded_len as u64,
            plan_metadata_bytes: 0,
            chunks: 0,
            longest_natural_run: 0,
            literal_bytes: bytes.len() as u64,
            philosophical_user_value_bytes_stored: bytes.len() as u64,
            metadata_amplification_ratio: None,
            average_chunk_len: None,
            philosophical_compression_ratio: raw_philosophical_ratio(bytes.len() as u64),
            purity: Purity::Contaminated,
        },
        ValueEnvelope::Plan(plan) => {
            let mut longest = 0_u32;
            let mut literal_bytes = 0_u64;
            for chunk in &plan.chunks {
                match chunk {
                    ChunkRef::Located { len, .. } => longest = longest.max(*len),
                    ChunkRef::Literal { bytes } => {
                        literal_bytes += bytes.len() as u64;
                        longest = longest.max(bytes.len() as u32);
                    }
                }
            }

            let ratio = if plan.logical_len == 0 {
                None
            } else {
                Some(encoded_len as f64 / plan.logical_len as f64)
            };

            ExplainValue {
                storage_class: StorageClass::Plan,
                logical_user_bytes: plan.logical_len,
                physical_value_bytes: encoded_len as u64,
                plan_metadata_bytes: encoded_len as u64,
                chunks: plan.chunks.len() as u64,
                longest_natural_run: longest,
                literal_bytes,
                philosophical_user_value_bytes_stored: literal_bytes,
                metadata_amplification_ratio: ratio,
                average_chunk_len: average_chunk_len(plan.logical_len, plan.chunks.len() as u64),
                philosophical_compression_ratio: plan_philosophical_ratio(
                    plan.logical_len,
                    literal_bytes,
                ),
                purity: if literal_bytes == 0 {
                    Purity::Pure
                } else {
                    Purity::Contaminated
                },
            }
        }
    }
}

fn average_chunk_len(logical_len: u64, chunks: u64) -> Option<f64> {
    if chunks == 0 {
        None
    } else {
        Some(logical_len as f64 / chunks as f64)
    }
}

fn raw_philosophical_ratio(logical_len: u64) -> PhilosophicalCompressionRatio {
    if logical_len == 0 {
        PhilosophicalCompressionRatio::Undefined
    } else {
        PhilosophicalCompressionRatio::Finite(1.0)
    }
}

fn plan_philosophical_ratio(logical_len: u64, literal_bytes: u64) -> PhilosophicalCompressionRatio {
    if logical_len == 0 {
        PhilosophicalCompressionRatio::Undefined
    } else if literal_bytes == 0 {
        PhilosophicalCompressionRatio::Infinite
    } else {
        PhilosophicalCompressionRatio::Revoked
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::plan::{
        ChunkRef, ChunkTransform, LogicalHash, LogicalHashKind, PlanCodec, PlanStream,
        ReconstructionPlan, StreamId,
    };

    #[test]
    fn raw_values_report_finite_philosophical_ratio() {
        let explain = explain_envelope(&ValueEnvelope::Raw(Bytes::from_static(b"abc")), 42);

        assert_eq!(explain.literal_bytes, 3);
        assert_eq!(explain.average_chunk_len, None);
        assert_eq!(
            explain.philosophical_compression_ratio,
            PhilosophicalCompressionRatio::Finite(1.0)
        );
    }

    #[test]
    fn pure_plans_report_infinite_philosophical_ratio() {
        let plan = plan_with_chunks(
            4,
            vec![ChunkRef::Located {
                stream_ix: 0,
                offset: 0,
                len: 4,
                transform: ChunkTransform::Identity,
            }],
        );
        let explain = explain_envelope(&ValueEnvelope::Plan(plan), 100);

        assert_eq!(explain.literal_bytes, 0);
        assert_eq!(explain.average_chunk_len, Some(4.0));
        assert_eq!(
            explain.philosophical_compression_ratio,
            PhilosophicalCompressionRatio::Infinite
        );
    }

    #[test]
    fn literal_chunks_revoke_philosophical_ratio() {
        let plan = plan_with_chunks(
            2,
            vec![ChunkRef::Literal {
                bytes: Bytes::from_static(b"ab"),
            }],
        );
        let explain = explain_envelope(&ValueEnvelope::Plan(plan), 100);

        assert_eq!(explain.literal_bytes, 2);
        assert_eq!(explain.average_chunk_len, Some(2.0));
        assert_eq!(
            explain.philosophical_compression_ratio,
            PhilosophicalCompressionRatio::Revoked
        );
    }

    fn plan_with_chunks(logical_len: u64, chunks: Vec<ChunkRef>) -> ReconstructionPlan {
        ReconstructionPlan {
            logical_len,
            logical_hash: LogicalHash::new(LogicalHashKind::Blake3_128, b""),
            planner_version: 1,
            plan_codec: PlanCodec::CompactBinary,
            streams: vec![PlanStream {
                id: StreamId::Sha256CounterV1 { seed: [0_u8; 32] },
                fingerprint: [0_u8; 32],
            }],
            chunks,
        }
    }
}
