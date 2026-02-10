WITH src AS (
    SELECT
        *
    FROM
        {{ source(
            'klt',
            'kobo_audit_log'
        ) }}
)
SELECT
    _dlt_id AS audit_load_id,
    TO_TIMESTAMP(
        _dlt_load_id :: DOUBLE PRECISION
    ) AT TIME ZONE 'UTC' AS loaded_at,
    date_created AT TIME ZONE 'UTC' AS created_at,
    user_uid AS user_id,
    username AS user_name,
    action,
    metadata__asset_uid AS asset_id,
    metadata__uuid AS submission_uuid
FROM
    src
WHERE
    log_type = 'submission-management' OR 
    log_type = 'project-history'
