package io.activej.redis;

import org.jetbrains.annotations.NotNull;

public final class ServerError extends RedisException {
	public ServerError(@NotNull String message) {
		super(RedisAPI.class, message);
	}
}
