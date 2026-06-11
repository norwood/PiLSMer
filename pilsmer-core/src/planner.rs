use std::sync::Arc;

use bytes::Bytes;

use crate::codec::varint_len;
use crate::error::{PiLsmError, Result};
use crate::plan::{
    ChunkRef, ChunkTransform, LogicalHash, LogicalHashKind, PlanCodec, PlanStream,
    ReconstructionPlan,
};
use crate::reconstruct::Reconstructor;
use crate::stream::StreamRegistry;
use crate::stream_index::StreamIndex;

const PLANNER_VERSION: u16 = 1;

#[derive(Clone, Debug)]
pub struct PlanOptions {
    pub max_prefix_len: u64,
    pub max_k: usize,
    pub max_index_memory_bytes: u64,
    pub max_offsets_per_kgram: u16,
    pub max_plan_millis_per_value: u64,
    pub allow_literals: bool,
    pub plan_codec: PlanCodec,
    pub logical_hash_kind: LogicalHashKind,
}

impl Default for PlanOptions {
    fn default() -> Self {
        Self {
            max_prefix_len: 16 * 1024 * 1024,
            max_k: 3,
            max_index_memory_bytes: 256 * 1024 * 1024,
            max_offsets_per_kgram: 4,
            max_plan_millis_per_value: 500,
            allow_literals: false,
            plan_codec: PlanCodec::CompactBinary,
            logical_hash_kind: LogicalHashKind::Blake3_128,
        }
    }
}

#[derive(Clone)]
pub struct Planner {
    indexes: Vec<Arc<StreamIndex>>,
    registry: StreamRegistry,
    options: PlanOptions,
}

impl Planner {
    pub fn new(
        indexes: Vec<Arc<StreamIndex>>,
        registry: StreamRegistry,
        options: PlanOptions,
    ) -> Self {
        Self {
            indexes,
            registry,
            options,
        }
    }

    pub async fn plan(&self, bytes: &[u8]) -> Result<ReconstructionPlan> {
        self.plan_with_options_ref(bytes, &self.options).await
    }

    pub async fn plan_with_options(
        &self,
        bytes: &[u8],
        options: PlanOptions,
    ) -> Result<ReconstructionPlan> {
        self.plan_with_options_ref(bytes, &options).await
    }

    async fn plan_with_options_ref(
        &self,
        bytes: &[u8],
        options: &PlanOptions,
    ) -> Result<ReconstructionPlan> {
        let streams = self
            .indexes
            .iter()
            .map(|index| PlanStream {
                id: index.stream_id().clone(),
                fingerprint: index.stream_fingerprint(),
            })
            .collect::<Vec<_>>();

        let choices = self.choose_chunks(bytes, options)?;
        let mut chunks = Vec::with_capacity(choices.len());
        for choice in choices {
            match choice {
                Choice::Located {
                    stream_ix,
                    offset,
                    len,
                    ..
                } => chunks.push(ChunkRef::Located {
                    stream_ix: stream_ix as u32,
                    offset,
                    len: len as u32,
                    transform: ChunkTransform::Identity,
                }),
                Choice::Literal(byte) => chunks.push(ChunkRef::Literal {
                    bytes: Bytes::copy_from_slice(&[byte]),
                }),
            }
        }

        let plan = ReconstructionPlan {
            logical_len: bytes.len() as u64,
            logical_hash: LogicalHash::new(options.logical_hash_kind, bytes),
            planner_version: PLANNER_VERSION,
            plan_codec: options.plan_codec,
            streams,
            chunks,
        };

        let reconstructed = Reconstructor::new(self.registry.clone())
            .reconstruct(&plan)
            .await?;
        if reconstructed.as_ref() != bytes {
            return Err(PiLsmError::LogicalHashMismatch);
        }

        Ok(plan)
    }

    fn choose_chunks(&self, bytes: &[u8], options: &PlanOptions) -> Result<Vec<Choice>> {
        let n = bytes.len();
        let mut dp: Vec<Option<State>> = vec![None; n + 1];
        dp[n] = Some(State {
            score: Score::zero(),
            choice: None,
        });

        for i in (0..n).rev() {
            let mut best: Option<State> = None;
            for candidate in self.candidates_at(bytes, i, options) {
                let next = i + candidate.len();
                let Some(next_state) = &dp[next] else {
                    continue;
                };
                let score = candidate.score().combine(next_state.score);
                let state = State {
                    score,
                    choice: Some(candidate),
                };
                if best
                    .as_ref()
                    .is_none_or(|current| state.score < current.score)
                {
                    best = Some(state);
                }
            }
            dp[i] = best;
        }

        let mut out = Vec::new();
        let mut i = 0;
        while i < n {
            let state = dp[i]
                .as_ref()
                .ok_or(PiLsmError::PlanningFailed("target cannot be covered"))?;
            let choice = state
                .choice
                .clone()
                .ok_or(PiLsmError::PlanningFailed("missing planner choice"))?;
            i += choice.len();
            out.push(choice);
        }

        Ok(out)
    }

    fn candidates_at(&self, bytes: &[u8], pos: usize, options: &PlanOptions) -> Vec<Choice> {
        let remaining = bytes.len() - pos;
        let max_k = options.max_k.min(remaining);
        let mut candidates = Vec::new();

        for len in 1..=max_k {
            let needle = &bytes[pos..pos + len];
            for (stream_ix, index) in self.indexes.iter().enumerate() {
                for found in index.find_candidates(needle) {
                    candidates.push(Choice::Located {
                        stream_ix,
                        offset: found.offset,
                        len: found.len as usize,
                        encoded_cost: found.encoded_cost,
                        read_cost_hint: found.read_cost_hint,
                    });
                }
            }
        }

        if options.allow_literals {
            candidates.push(Choice::Literal(bytes[pos]));
        }

        candidates
    }
}

#[derive(Clone, Debug)]
enum Choice {
    Located {
        stream_ix: usize,
        offset: u64,
        len: usize,
        encoded_cost: u16,
        read_cost_hint: u16,
    },
    Literal(u8),
}

impl Choice {
    fn len(&self) -> usize {
        match self {
            Choice::Located { len, .. } => *len,
            Choice::Literal(_) => 1,
        }
    }

    fn score(&self) -> Score {
        match self {
            Choice::Located {
                encoded_cost,
                read_cost_hint,
                ..
            } => Score {
                encoded_bytes: u64::from(*encoded_cost),
                chunks: 1,
                read_cost: u64::from(*read_cost_hint),
            },
            Choice::Literal(_) => Score {
                encoded_bytes: 3 + u64::from(varint_len(1)),
                chunks: 1,
                read_cost: 0,
            },
        }
    }
}

#[derive(Clone, Debug)]
struct State {
    score: Score,
    choice: Option<Choice>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Score {
    encoded_bytes: u64,
    chunks: u64,
    read_cost: u64,
}

impl Score {
    fn zero() -> Self {
        Self {
            encoded_bytes: 0,
            chunks: 0,
            read_cost: 0,
        }
    }

    fn combine(self, other: Self) -> Self {
        Self {
            encoded_bytes: self.encoded_bytes + other.encoded_bytes,
            chunks: self.chunks + other.chunks,
            read_cost: self.read_cost + other.read_cost,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::stream::{ByteStream, PrefixByteStream};
    use crate::stream_index::{StreamIndex, StreamIndexOptions};
    use crate::StreamId;

    fn prefix_stream(prefix: &'static [u8]) -> Arc<dyn ByteStream> {
        Arc::new(PrefixByteStream::new(
            StreamId::PiHexFractionPrefixV1 {
                digest: [3_u8; 32],
                bytes: prefix.len() as u64,
            },
            Bytes::from_static(prefix),
        ))
    }

    #[tokio::test]
    async fn planner_uses_dp_instead_of_greedy_longest_match() {
        let stream = prefix_stream(b"ABCDqABCpDEF");
        let mut registry = StreamRegistry::new();
        registry.register(stream.clone());
        let index = Arc::new(
            StreamIndex::build(
                stream,
                StreamIndexOptions {
                    max_prefix_len: 12,
                    max_k: 4,
                    max_index_memory_bytes: 8192,
                    max_offsets_per_kgram: 4,
                },
            )
            .await
            .unwrap(),
        );
        let planner = Planner::new(
            vec![index],
            registry,
            PlanOptions {
                max_k: 4,
                ..PlanOptions::default()
            },
        );

        let plan = planner.plan(b"ABCDEF").await.unwrap();
        assert_eq!(plan.chunks.len(), 2);
        assert!(matches!(plan.chunks[0], ChunkRef::Located { len: 3, .. }));
        assert!(matches!(plan.chunks[1], ChunkRef::Located { len: 3, .. }));
    }

    #[tokio::test]
    async fn planner_can_use_literals_when_allowed() {
        let stream = prefix_stream(b"abc");
        let mut registry = StreamRegistry::new();
        registry.register(stream.clone());
        let index = Arc::new(
            StreamIndex::build(
                stream,
                StreamIndexOptions {
                    max_prefix_len: 3,
                    max_k: 2,
                    max_index_memory_bytes: 1024,
                    max_offsets_per_kgram: 2,
                },
            )
            .await
            .unwrap(),
        );
        let planner = Planner::new(
            vec![index],
            registry,
            PlanOptions {
                max_k: 2,
                allow_literals: true,
                ..PlanOptions::default()
            },
        );

        let plan = planner.plan(b"az").await.unwrap();
        assert!(matches!(
            plan.chunks.last(),
            Some(ChunkRef::Literal { bytes }) if bytes.as_ref() == b"z"
        ));
    }
}
