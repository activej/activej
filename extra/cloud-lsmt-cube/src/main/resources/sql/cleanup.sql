SET autocommit = 0;

START TRANSACTION;

SET @start_revision = (SELECT MAX(`revision`)
                       FROM {revision} r
                                INNER JOIN
                                (SELECT MAX(`revision`) - {min_revisions} as `max_revision` FROM {revision}) g
                                ON r.`revision` <= g.`max_revision`
                       WHERE `created_at` < NOW() - INTERVAL {cleanup_from});

DELETE
FROM {revision}
WHERE `revision` <= @start_revision;

DELETE
FROM {chunk}
WHERE `removed_revision` <= @start_revision;

DELETE p
FROM (SELECT `partition_id`, MAX(`revision_id`) as `max_rev`
      FROM {position}
      WHERE `revision_id` < @start_revision
      GROUP BY `partition_id`) g
         LEFT JOIN {position} p
ON p.`partition_id` = g.`partition_id`
WHERE p.`revision_id` < g.`max_rev`;

COMMIT;
