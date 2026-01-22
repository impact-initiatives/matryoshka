WITH src AS (
    SELECT
        *
    FROM
        {{ source(
            'klt',
            'kobo_asset_for_submissions'
        ) }}
)
SELECT
    uid AS asset_id,
    _dlt_id AS asset_load_id,
    TO_TIMESTAMP(
        _dlt_load_id :: DOUBLE PRECISION
    ) AT TIME ZONE 'UTC' AS loaded_at,
    deployment__last_submission_time AT TIME ZONE 'UTC' AS last_submission_at,
    deployment__submission_count AS count_submissions
FROM
    src
