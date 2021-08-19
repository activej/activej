CREATE TABLE IF NOT EXISTS `{chunk}`
(
    `id`               BIGINT       NOT NULL,
    `aggregation`      VARCHAR(255) NOT NULL,
    `measures`         TEXT         NOT NULL,
    `min_key`          TEXT         NOT NULL,
    `max_key`          TEXT         NOT NULL,
    `item_count`       INT          NOT NULL,
    `added_revision`   BIGINT       NOT NULL,
    `removed_revision` BIGINT       NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
   DEFAULT CHARSET = utf8;

DROP TRIGGER IF EXISTS `{chunk}_remove_check`;

CREATE TRIGGER `{chunk}_remove_check`
   AFTER UPDATE
   ON {chunk}
   FOR EACH ROW
BEGIN
   IF OLD.`removed_revision` IS NOT NULL THEN
       SIGNAL SQLSTATE '23513'
           SET MESSAGE_TEXT = 'Chunk is already removed';
   END IF;
END;
