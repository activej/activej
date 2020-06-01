CREATE TABLE IF NOT EXISTS `{diffs}`
(
    `revision_id` bigint unsigned NOT NULL,
    `parent_id`   bigint          NOT NULL,
    `diff`        longtext        NOT NULL,
    PRIMARY KEY (`revision_id`, `parent_id`),
    UNIQUE KEY `parent_id` (`parent_id`, `revision_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
