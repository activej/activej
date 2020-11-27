package io.activej.redis.api;

import org.jetbrains.annotations.NotNull;

public final class ServerError extends ExpectedRedisException {
	public ServerError(@NotNull String message) {
		super(message);
	}
}
