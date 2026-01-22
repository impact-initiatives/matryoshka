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
    _dlt_id AS submission_load_id,
    TO_TIMESTAMP(
        _dlt_load_id :: DOUBLE PRECISION
    ) AT TIME ZONE 'UTC' AS loaded_at
FROM
    src
