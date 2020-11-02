package io.activej.redis;

import io.activej.common.exception.StacklessException;
import org.jetbrains.annotations.NotNull;

public class RedisException extends StacklessException {
	public RedisException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
