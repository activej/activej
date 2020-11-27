package io.activej.redis;

import org.jetbrains.annotations.NotNull;

public class ExpectedRedisException extends RedisException {
	public ExpectedRedisException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}

	public ExpectedRedisException(@NotNull Class<?> component, @NotNull String message, @NotNull Throwable cause) {
		super(component, message, cause);
	}
}
