WITH src AS (
    SELECT
        *
    FROM
        {{ source(
            'klt',
            'kobo_submission'
        ) }}
)
SELECT
    _id AS submission_id,
    _uuid AS submission_uuid,
    _dlt_id AS submission_load_id,
    asset_uid AS asset_id,
    TO_TIMESTAMP(
        _dlt_load_id :: DOUBLE PRECISION
    ) AT TIME ZONE 'UTC' AS loaded_at,
    _submission_time AT TIME ZONE 'utc' AS submitted_at
FROM
    src
