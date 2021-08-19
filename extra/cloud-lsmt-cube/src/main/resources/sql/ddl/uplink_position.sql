CREATE TABLE IF NOT EXISTS `{position}`
(
    `revision_id`  BIGINT       NOT NULL,
    `partition_id` VARCHAR(255) NOT NULL,
    `filename`     VARCHAR(255) NOT NULL,
    `remainder`    INT          NOT NULL,
    `position`     BIGINT       NOT NULL
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8;
