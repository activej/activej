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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;

/**
 * Encapsulation of certain fatal error handlers that determine behaviour in case of fatal error
 * occurrences.
 */
public final class FatalErrorHandlers {
	private static final Logger logger = LoggerFactory.getLogger(FatalErrorHandlers.class);

	private static volatile FatalErrorHandler globalFatalErrorHandler = FatalErrorHandlers.rethrowOnAnyError();

	private static final ThreadLocal<FatalErrorHandler> CURRENT_HANDLER = new ThreadLocal<>();

	public static void setThreadFatalErrorHandler(@NotNull FatalErrorHandler handler) {
		CURRENT_HANDLER.set(handler);
	}

	public static void setGlobalFatalErrorHandler(@NotNull FatalErrorHandler handler) {
		globalFatalErrorHandler = handler;
	}

	public static @NotNull FatalErrorHandler getThreadFatalErrorHandler() {
		FatalErrorHandler handler = CURRENT_HANDLER.get();
		return handler == null ? globalFatalErrorHandler : handler;
	}

	public static void handleRuntimeException(@NotNull Exception e, @Nullable Object context) {
		if (e instanceof RuntimeException) {
			getThreadFatalErrorHandler().handle(e, context);
		}
	}

	public static void handleRuntimeException(@NotNull Exception e) {
		handleRuntimeException(e, null);
	}

	public static FatalErrorHandler ignoreAllErrors() {
		return (e, context) -> {};
	}

	public static FatalErrorHandler exitOnAnyError() {
		return (e, context) -> shutdownForcibly();
	}

	public static FatalErrorHandler exitOnMatchedError(Predicate<Throwable> predicate) {
		return (e, context) -> {
			if (predicate.test(e)) {
				shutdownForcibly();
			}
		};
	}

	public static FatalErrorHandler exitOnMatchedError(Class<? extends Throwable> cls) {
		return exitOnMatchedError(throwable -> cls.isAssignableFrom(throwable.getClass()));
	}

	public static FatalErrorHandler exitOnJvmError() {
		return exitOnMatchedError(e -> e instanceof Error);
	}

	public static FatalErrorHandler rethrowOnAnyError() {
		return (e, context) -> propagate(e);
	}

	public static FatalErrorHandler rethrowOnMatchedError(Predicate<Throwable> predicate) {
		return (e, context) -> {
			if (predicate.test(e)) {
				propagate(e);
			}
		};
	}

	public static FatalErrorHandler rethrowOnMatchedError(Class<? extends Throwable> cls) {
		return rethrowOnMatchedError(throwable -> cls.isAssignableFrom(throwable.getClass()));
	}

	public static FatalErrorHandler logging() {
		return logging(logger);
	}

	public static FatalErrorHandler logging(Logger logger) {
		return (e, context) -> {
			if (!logger.isErrorEnabled()) return;

			if (context == null) {
				logger.error("Fatal error", e);
			} else {
				logger.error("Fatal error in {}", context, e);
			}
		};
	}

	public static void propagate(@NotNull Throwable e) {
		if (e instanceof Error) {
			throw (Error) e;
		} else if (e instanceof RuntimeException) {
			throw (RuntimeException) e;
		} else {
			throw new RuntimeException(e);
		}
	}

	private static void shutdownForcibly() {
		Runtime.getRuntime().halt(1);
	}
}
