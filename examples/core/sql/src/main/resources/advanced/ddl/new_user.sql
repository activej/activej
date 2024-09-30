CREATE TABLE IF NOT EXISTS `new_users` (
    `id`   INTEGER      NOT NULL AUTO_INCREMENT,
    `first_name` VARCHAR(128) NOT NULL,
    `last_name` VARCHAR(128) NOT NULL,
    PRIMARY KEY (`id`)
);
