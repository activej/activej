package io.activej.redis;

import org.jetbrains.annotations.NotNull;

public final class ServerError extends ExpectedRedisException {
	public ServerError(@NotNull String message) {
		super(RedisApi.class, message);
	}
}
