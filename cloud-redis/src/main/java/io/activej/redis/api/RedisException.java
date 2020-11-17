package io.activej.redis.api;

import io.activej.common.exception.StacklessException;
import org.jetbrains.annotations.NotNull;

public class RedisException extends StacklessException {
	public RedisException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}

	public RedisException(@NotNull Class<?> component, @NotNull String message, @NotNull Throwable cause) {
		super(component, message, cause);
	}
}
