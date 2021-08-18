CREATE TABLE IF NOT EXISTS `{lock}`
(
    `aggregation_id` varchar(255)                         not null,
    `chunk_id`       varchar(255)                         not null,
    `locked_at`      timestamp default CURRENT_TIMESTAMP not null,
    `locked_by`      varchar(255)                         null,
    PRIMARY KEY (chunk_id, aggregation_id)
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8;

