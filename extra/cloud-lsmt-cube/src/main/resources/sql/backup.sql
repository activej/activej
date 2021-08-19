SET autocommit = 0;

START TRANSACTION;

INSERT INTO {backup} (`revision`, `created_at`, `created_by`, `backup_by`)
SELECT {backup_revision}, `created_at`, `created_by`, {backup_by} FROM {revision} WHERE `revision`={backup_revision};

INSERT INTO {backup_chunk}
SELECT {backup_revision},
       `id`,
       `aggregation`,
       `measures`,
       `min_key`,
       `max_key`,
       `item_count`,
       `added_revision`
FROM {chunk}
WHERE `added_revision` <= {backup_revision} AND (`removed_revision` IS NULL OR `removed_revision` > {backup_revision});

INSERT INTO {backup_position}
SELECT {backup_revision}, p.*
FROM {position} p
INNER JOIN
 (SELECT `partition_id`, MAX(`revision_id`) AS `max_revision`
 FROM {position}
 WHERE `revision_id` <= {backup_revision}
 GROUP BY `partition_id`) g
ON p.`partition_id` = g.`partition_id`
AND p.`revision_id` = g.`max_revision`;

-- COMMIT; -commit will be made after chunk files are backed up
