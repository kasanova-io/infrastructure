//! The shared journal owns chain ingestion. Retain legacy chain storage without
//! loading its scanner cache or starting its maintenance workers.
use fjall::TxKeyspace;
use indexer_actors::virtual_chain_processor::CompactHeader;
use indexer_db::headers::block_compact_headers::BlockCompactHeaderPartition;
use indexer_db::headers::block_gaps::{BlockGap, BlockGapsPartition};
use indexer_db::headers::daa_index::DaaIndexPartition;
use kaspa_rpc_core::RpcBlueWorkType;

pub struct ChainStartup {
    pub gaps: Vec<BlockGap>,
    pub blocks_processed: u64,
    pub processed_blocks: Vec<CompactHeader>,
}

pub fn chain_startup(
    shared: bool,
    db: &TxKeyspace,
    gaps: &BlockGapsPartition,
    headers: &BlockCompactHeaderPartition,
    daa_index: &DaaIndexPartition,
) -> anyhow::Result<Option<ChainStartup>> {
    if shared {
        tracing::info!("Shared ingestion: legacy chain cache and maintenance workers disabled");
        return Ok(None);
    }
    let gaps = gaps.get_all_gaps().collect::<Result<Vec<_>, _>>()?;
    let blocks_processed = headers.len()? as u64;
    tracing::info!("start filling processed block cache");
    let read = db.read_tx();
    let processed_blocks = daa_index
        .iter_lt(&read, u64::MAX)
        .rev()
        .map(|row| {
            row.map(|key| CompactHeader {
                block_hash: key.block_hash,
                blue_work: RpcBlueWorkType::from_be_bytes(key.blue_work),
                daa_score: key.daa_score.get(),
            })
        })
        .take(3_000_000)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(ChainStartup {
        gaps,
        blocks_processed,
        processed_blocks,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::{Config, PartitionCreateOptions};

    #[test]
    fn shared_startup_ignores_legacy_chain_decode_errors_and_retains_user_data()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let db = Config::new(temp.path()).open_transactional()?;
        let gaps = BlockGapsPartition::new(&db)?;
        let headers = BlockCompactHeaderPartition::new(&db)?;
        let daa = DaaIndexPartition::new(&db)?;
        let legacy = db.open_partition("block_gaps", PartitionCreateOptions::default())?;
        let registrations = db.open_partition("device_registration", PartitionCreateOptions::default())?;
        let messages = db.open_partition("tx-id-to-contextual-message", PartitionCreateOptions::default())?;
        let mut write = db.write_tx()?;
        write.insert(&legacy, b"unused-invalid-chain-key", b"unused-invalid-chain-value");
        write.insert(&registrations, b"synthetic-registration", b"retained-registration");
        write.insert(&messages, [7u8; 32], [0xff, 0, b':']);
        write.commit()?.map_err(|_| anyhow::anyhow!("fixture conflict"))?;
        assert!(chain_startup(false, &db, &gaps, &headers, &daa).is_err());
        assert!(chain_startup(true, &db, &gaps, &headers, &daa)?.is_none());
        let read = db.read_tx();
        assert_eq!(read.get(&registrations, b"synthetic-registration")?.unwrap().as_ref(), b"retained-registration");
        assert_eq!(read.get(&messages, [7u8; 32])?.unwrap().as_ref(), &[0xff, 0, b':']);
        assert_eq!(read.get(&legacy, b"unused-invalid-chain-key")?.unwrap().as_ref(), b"unused-invalid-chain-value");
        Ok(())
    }

    #[test]
    fn standalone_startup_retains_legacy_counts_gaps_and_descending_cache()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let db = Config::new(temp.path()).open_transactional()?;
        let gaps = BlockGapsPartition::new(&db)?;
        let headers = BlockCompactHeaderPartition::new(&db)?;
        let daa = DaaIndexPartition::new(&db)?;
        gaps.add_gap(BlockGap { from: [1; 32], to: [2; 32] })?;
        for n in [1u8, 2, 3] {
            headers.insert_compact_header(&[n; 32], [0; 24], u64::from(n))?;
            daa.insert(u64::from(n), [n; 32], [0; 24])?;
        }
        let before = chain_startup(false, &db, &gaps, &headers, &daa)?.unwrap();
        assert_eq!(before.blocks_processed, 3);
        assert_eq!(before.gaps.len(), 1);
        assert_eq!(before.gaps[0].from, [1; 32]);
        assert_eq!(before.gaps[0].to, [2; 32]);
        assert_eq!(before.processed_blocks.iter().map(|h| h.daa_score).collect::<Vec<_>>(), vec![3, 2, 1]);
        assert!(chain_startup(true, &db, &gaps, &headers, &daa)?.is_none());
        let after = chain_startup(false, &db, &gaps, &headers, &daa)?.unwrap();
        assert_eq!(after.blocks_processed, before.blocks_processed);
        assert_eq!(after.processed_blocks.iter().map(|h| h.block_hash).collect::<Vec<_>>(), before.processed_blocks.iter().map(|h| h.block_hash).collect::<Vec<_>>());
        Ok(())
    }
}
