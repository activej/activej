/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.common.exception;

import org.jetbrains.annotations.NotNull;

/**
 * This exception (as well as its subtypes) is used in asynchronous contexts
 * where the default Java call stacktrace has no useful meaning and is redundant
 * to fetch, store or print.
 */

public class StacklessException extends Exception {
	@NotNull
	private final Class<?> component;

	public StacklessException(@NotNull Class<?> component, @NotNull String message) {
		super(message);
		this.component = component;
	}

	public StacklessException(@NotNull Class<?> component, @NotNull String message, @NotNull Throwable cause) {
		super(message, cause);
		this.component = component;
	}

	@NotNull
	public Class<?> getComponent() {
		return component;
	}

	@Override
	public final Throwable fillInStackTrace() {
		return this;
	}

	@Override
	public String toString() {
		return getClass().getName() +
				" (" + component.getSimpleName() + ")" + " : " +
				getMessage();
	}
}
