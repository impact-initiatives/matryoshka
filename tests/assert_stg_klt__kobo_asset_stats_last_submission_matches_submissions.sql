WITH computed_stats AS (
    SELECT
        submissions.asset_id,
        DATE_TRUNC('second', MAX(submissions.submitted_at))
            AS computed_last_submission_at
    FROM {{ ref('stg_klt__kobo_submission') }} AS submissions
    GROUP BY submissions.asset_id
),

observed_stats AS (
    SELECT
        stats.asset_id,
        DATE_TRUNC('second', stats.last_submission_at)
            AS observed_last_submission_at
    FROM {{ ref('stg_klt__kobo_asset_stats') }} AS stats
),

last_deletion_per_asset AS (
    SELECT
        asset_id,
        MAX(created_at) AS last_deleted_at
    FROM {{ ref('stg_klt__kobo_audit_log__submission_management') }}
    WHERE action LIKE '%delete%'
    GROUP BY asset_id
),

joined AS (
    SELECT
        computed_stats.asset_id,
        computed_stats.computed_last_submission_at,
        observed_stats.observed_last_submission_at,
        last_deletion_per_asset.last_deleted_at
    FROM computed_stats
    JOIN observed_stats
        ON computed_stats.asset_id = observed_stats.asset_id
    LEFT JOIN last_deletion_per_asset
        ON computed_stats.asset_id = last_deletion_per_asset.asset_id
)

SELECT
    asset_id,
    computed_last_submission_at,
    observed_last_submission_at,
    last_deleted_at
FROM joined
WHERE
    -- Case 1: API knows about submissions we don't have,
    -- and no deletion after the observed stat explains the gap
    (
        computed_last_submission_at < observed_last_submission_at
        AND (
            last_deleted_at IS NULL
            OR last_deleted_at <= observed_last_submission_at
        )
    )

    OR (
        -- Case 2: we have newer data than API reports,
        -- recent enough that audit logs should cover it,
        -- and no deletion explains it
        computed_last_submission_at > observed_last_submission_at
        AND observed_last_submission_at >= CURRENT_DATE - INTERVAL '1 year'
        AND (
            last_deleted_at IS NULL
            OR last_deleted_at < observed_last_submission_at
        )
    )
