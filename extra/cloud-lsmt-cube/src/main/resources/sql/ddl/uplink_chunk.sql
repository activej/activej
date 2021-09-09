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
    `locked_at`        TIMESTAMP    NULL,
    `locked_by`        VARCHAR(255) NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
   DEFAULT CHARSET = utf8;

DROP TRIGGER IF EXISTS `{chunk}_remove_check`;

CREATE TRIGGER `{chunk}_remove_check`
   BEFORE UPDATE
   ON {chunk}
   FOR EACH ROW
BEGIN
   IF OLD.`removed_revision` IS NOT NULL THEN
       SET @message_text = CONCAT('Chunk ', OLD.id, ' is already removed in revision ', OLD.removed_revision);
       SIGNAL SQLSTATE '23513'
           SET MESSAGE_TEXT = @message_text;
   END IF;
   IF NEW.`removed_revision` IS NOT NULL THEN
        SET NEW.`locked_at` = NULL, NEW.`locked_by` = NULL;
    END IF;
END;
