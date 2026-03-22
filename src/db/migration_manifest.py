from __future__ import annotations


TRUSTED_MIGRATION_HASHES = {
    "0001_extensions.sql": "b5d8d031970cedb543a0b9df4a0d7e7e928a8803e286254fcbeb321741fc9afa",
    "0002_runtime_tables.sql": "7b252c0a453d699e4f6793d643e33a2a2cecbc335790499b1fb3e0b484aeda24",
    "0003_bronze_hyperliquid.sql": "6f1eb58e6ad1417c08490697e9a7b53001cd354eb41ca74800821d40740660c4",
    "0004_bronze_binance_market.sql": "6db10528f35b88f3107c9b456a499795dc606981488ea2f1a5b669c22d1cf567",
    "0005_bronze_binance_streams.sql": "1fbee51d9307bd0430a91504595f788d2c7930a4b66cc675298e25a778f7deab",
    "0006_bronze_reference_data.sql": "79708091d2410f782153298cc640060771e7b92715d0af6d22911a069b7eb391",
    "0007_bronze_s3.sql": "232338ca4c4be5f88b01b403b7cb9b91d5cf2164b20a9187f31b7be19a5b03c7",
    "0008_bronze_binance_rest_metrics.sql": "5b8bebb62f7732d45868012e3104b282981547d461f6ae283baa41a2fd810eb6",
    "0009_bronze_binance_rest_reference.sql": "ae5120b79308f666b9ce239ebbe74c3773b6512664375f341579058e303a91f7",
    "0010_bronze_compression.sql": "81ca0dbf34f944867cf2864854550703b3cce142fefaa78901f81bf5fe9b2098",
    "0011_runtime_default_repairs.sql": "44fe06b7401de3abfd01a1690be2a52f8e592b0f1842447becc595509ef5bbbe",
    "0012_bronze_dedup_repairs.sql": "f0344008447997139a12801ca02bb3f0a3872058cb2be33e1085703f9287d777",
    "0013_ws_replay_registry.sql": "6fa798dec142add145558405164cd95fbc391310243187602242e666132dbca1",
    "0014_news_depth_replay_fixes.sql": "9f4be3d42eafe0e51f74f7fc01e3f232208dee44ca15fba577ca5695904dee91",
    "0015_news_replay_repairs.sql": "52dd3fa452e4c0edd387c21a734e322f0e87e221f7ddf7d7d68d5a2cc812eaf9",
    "0016_trade_replay_repairs.sql": "5c5fd03f53702650bb3befb7199913567de2f7e959b839a26976fe6263115978",
    "0017_ws_replay_repairs.sql": "491dc85e16d44c796938148574ed3736475678436f090bf1ca82ec722e8aa6ee",
    "0018_reference_policy_repairs.sql": "6ade8610c391fa2ea69d4f7d4b4a8fa7e0accd9bc8e5ec3a17370b30e5fe5790",
    "0019_replay_guard_triggers.sql": "2528a77164e665fbbf88104321e668bcccba79088d4ec0458983456054887273",
    "0020_news_alias_dedup_backfill.sql": "50c05421030620b1bd6adf07ce3b5361441c9527ab641931767a5a33cce4cc57",
    "0021_delivery_null_quarantine.sql": "09b6d90a07121de7956a021ef72b93848f936530eaa44f88f344a1f30bb6c13e",
    "0022_news_path_alias_repair.sql": "1c639ce9fe1c8ca46321b9b640f2c8a609ca1d11fc5cdf64c5e169b6dab97641",
    "0023_delivery_null_bronze_restore.sql": "f052a3d6282e6723e8949760373c7957df723ac70a2f953ab8d89a0ba291fea8",
    "0024_reference_compression_policy_repair.sql": "cd70a58ed2ddf52f3a06486bc17c7cbef93ccc7bdabc686fb6aa0824f56b246a",
}

COMPATIBLE_MIGRATION_HASHES = {
    "0002_runtime_tables.sql": {
        "7b252c0a453d699e4f6793d643e33a2a2cecbc335790499b1fb3e0b484aeda24",
        "2634768c208594bf66b5172f926785bc764b3af35c88deee31c715a2c348ea69",
    }
}

STARTUP_NON_TRANSACTIONAL_MIGRATIONS = {
    "0012_bronze_dedup_repairs.sql",
    "0015_news_replay_repairs.sql",
    "0018_reference_policy_repairs.sql",
    "0020_news_alias_dedup_backfill.sql",
    "0022_news_path_alias_repair.sql",
    "0023_delivery_null_bronze_restore.sql",
}
