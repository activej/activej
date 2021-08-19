CREATE TABLE IF NOT EXISTS `backup`
(
    `revision`   BIGINT       NOT NULL,
    `created_at` TIMESTAMP    NOT NULL,
    `created_by` VARCHAR(255) NULL,
    `backup_started_at`   TIMESTAMP    NOT NULL  DEFAULT  CURRENT_TIMESTAMP,
    `backup_by`  VARCHAR(255) NULL,
    PRIMARY KEY (`revision`)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `backup_chunk`
(
    `backup_id`        BIGINT       NOT NULL,
    `id`               BIGINT       NOT NULL,
    `aggregation`      VARCHAR(255) NOT NULL,
    `measures`         TEXT         NOT NULL,
    `min_key`          TEXT         NOT NULL,
    `max_key`          TEXT         NOT NULL,
    `item_count`       INT          NOT NULL,
    `added_revision`   BIGINT       NOT NULL,
    PRIMARY KEY (`backup_id`,`id`)
) ENGINE = InnoDB
   DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS `backup_position`
(
    `backup_id`    BIGINT       NOT NULL,
    `revision_id`  BIGINT       NOT NULL,
    `partition_id` VARCHAR(255) NOT NULL,
    `filename`     VARCHAR(255) NOT NULL,
    `remainder`    INT          NOT NULL,
    `position`     BIGINT       NOT NULL,
    PRIMARY KEY (`backup_id`, `revision_id`, `partition_id`)
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8;

