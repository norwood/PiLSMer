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
                purity: if literal_bytes == 0 {
                    Purity::Pure
                } else {
                    Purity::Contaminated
                },
            }
        }
    }
}
