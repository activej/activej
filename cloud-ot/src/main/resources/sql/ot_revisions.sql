CREATE TABLE IF NOT EXISTS `{revisions}`
(
    `id`         bigint                      NOT NULL AUTO_INCREMENT,
    `epoch`      int                         NOT NULL,
    `level`      bigint                      NOT NULL,
    `snapshot`   longtext,
    `type`       enum ('NEW','HEAD','INNER') NOT NULL DEFAULT 'NEW',
    `timestamp`  timestamp                   NULL     DEFAULT CURRENT_TIMESTAMP,
    `created_by` varchar(100)                         DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
