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
    COALESCE(
        metadata__asset_uid,
        metadata__asset_file__uid
    ) AS asset_id,
    COALESCE(
	metadata__submission__root_uuid,
        metadata__instance__root_uuid,
        metadata__uuid
    ) AS submission_uuid,
    metadata__ip_address AS device_ip_address
FROM
    src
WHERE
    metadata__submission__root_uuid IS NOT NULL
    OR metadata__instance__root_uuid IS NOT NULL
    OR metadata__submission__status IS NOT NULL
    OR metadata__submission__submitted_by IS NOT NULL
    OR metadata__uuid IS NOT NULL
