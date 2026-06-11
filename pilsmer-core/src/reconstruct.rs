use bytes::{Bytes, BytesMut};

use crate::error::{PiLsmError, Result};
use crate::plan::{compute_logical_hash, ChunkRef, ChunkTransform, ReconstructionPlan};
use crate::stream::StreamRegistry;

#[derive(Clone)]
pub struct Reconstructor {
    registry: StreamRegistry,
}

impl Reconstructor {
    pub fn new(registry: StreamRegistry) -> Self {
        Self { registry }
    }

    pub async fn reconstruct(&self, plan: &ReconstructionPlan) -> Result<Bytes> {
        let cap = usize::try_from(plan.logical_len)
            .map_err(|_| PiLsmError::DecodeLimitExceeded("logical_len"))?;
        let mut out = BytesMut::with_capacity(cap);

        for chunk in &plan.chunks {
            match chunk {
                ChunkRef::Located {
                    stream_ix,
                    offset,
                    len,
                    transform,
                } => {
                    let stream_meta = plan
                        .streams
                        .get(*stream_ix as usize)
                        .ok_or(PiLsmError::MissingStream)?;
                    let stream = self
                        .registry
                        .get(&stream_meta.id)
                        .ok_or(PiLsmError::MissingStream)?;
                    if stream.fingerprint() != stream_meta.fingerprint {
                        return Err(PiLsmError::StreamFingerprintMismatch);
                    }

                    let mut bytes = stream.read_at(*offset, *len as usize).await?.to_vec();
                    apply_transform(&mut bytes, *transform);
                    out.extend_from_slice(&bytes);
                }
                ChunkRef::Literal { bytes } => out.extend_from_slice(bytes),
            }
        }

        if out.len() as u64 != plan.logical_len {
            return Err(PiLsmError::InvalidPlan("reconstructed length mismatch"));
        }

        if compute_logical_hash(plan.logical_hash.kind, &out) != plan.logical_hash.bytes {
            return Err(PiLsmError::LogicalHashMismatch);
        }

        Ok(out.freeze())
    }
}

fn apply_transform(bytes: &mut [u8], transform: ChunkTransform) {
    match transform {
        ChunkTransform::Identity => {}
        ChunkTransform::XorByte(mask) => {
            for byte in bytes {
                *byte ^= mask;
            }
        }
        ChunkTransform::Reverse => bytes.reverse(),
    }
}
