CREATE OR REPLACE VIEW idx_sw_l3_peers AS
WITH current_members AS (
    SELECT DISTINCT
        l1_code,
        l1_name,
        l2_code,
        l2_name,
        l3_code,
        l3_name,
        ts_code,
        name
    FROM idx_sw_member_all
    WHERE out_date IS NULL
      AND l3_code IS NOT NULL
),
anchor_l3_counts AS (
    SELECT
        ts_code,
        COUNT(DISTINCT l3_code) AS anchor_l3_count
    FROM current_members
    GROUP BY ts_code
),
peer_group_sizes AS (
    SELECT
        l3_code,
        COUNT(*) AS peer_group_size
    FROM current_members
    GROUP BY l3_code
)
SELECT
    anchor.ts_code AS anchor_ts_code,
    anchor.name AS anchor_name,
    anchor_counts.anchor_l3_count,
    anchor.l1_code,
    anchor.l1_name,
    anchor.l2_code,
    anchor.l2_name,
    anchor.l3_code,
    anchor.l3_name,
    peer_sizes.peer_group_size,
    peer.ts_code AS peer_ts_code,
    peer.name AS peer_name,
    peer.ts_code = anchor.ts_code AS peer_is_self
FROM current_members AS anchor
JOIN current_members AS peer
  ON anchor.l3_code = peer.l3_code
JOIN anchor_l3_counts AS anchor_counts
  ON anchor_counts.ts_code = anchor.ts_code
JOIN peer_group_sizes AS peer_sizes
  ON peer_sizes.l3_code = anchor.l3_code;