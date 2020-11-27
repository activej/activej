package io.activej.redis.api;

import org.jetbrains.annotations.NotNull;

public class ExpectedRedisException extends RedisException {
	public ExpectedRedisException(@NotNull String message) {
		super(RedisAPI.class, message);
	}
}
