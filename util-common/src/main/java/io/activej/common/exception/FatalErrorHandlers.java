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

import java.io.IOError;
import java.util.List;
import java.util.zip.ZipError;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Encapsulation of certain fatal error handlers that determine behaviour in case of fatal error
 * occurrences.
 */
public final class FatalErrorHandlers {
	private static volatile FatalErrorHandler globalFatalErrorHandler = FatalErrorHandlers.rethrowOnAnyError();

	private static final ThreadLocal<FatalErrorHandler> CURRENT_HANDLER = ThreadLocal.withInitial(() -> globalFatalErrorHandler);

	public static void setThreadFatalErrorHandler(@NotNull FatalErrorHandler handler) {
		CURRENT_HANDLER.set(handler);
	}

	public static void setGlobalFatalErrorHandler(@NotNull FatalErrorHandler handler) {
		globalFatalErrorHandler = handler;
	}

	public static @NotNull FatalErrorHandler getThreadFatalErrorHandler() {
		return CURRENT_HANDLER.get();
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

	public static FatalErrorHandler exitOnMatchedError(List<Class<?>> whiteList, List<Class<?>> blackList) {
		return (e, context) -> {
			if (matchesAny(e.getClass(), whiteList) && !matchesAny(e.getClass(), blackList)) {
				shutdownForcibly();
			}
		};
	}

	public static FatalErrorHandler exitOnJvmError() {
		return exitOnMatchedError(singletonList(Error.class), asList(AssertionError.class, StackOverflowError.class, IOError.class, ZipError.class));
	}

	public static FatalErrorHandler rethrowOnAnyError() {
		return (e, context) -> propagate(e);
	}

	public static FatalErrorHandler rethrowOnMatchedError(List<Class<?>> whiteList, List<Class<?>> blackList) {
		return (e, context) -> {
			if (matchesAny(e.getClass(), whiteList) && !matchesAny(e.getClass(), blackList)) {
				propagate(e);
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

	private static boolean matchesAny(Class<?> c, List<Class<?>> list) {
		return list.stream().anyMatch(cl -> cl.isAssignableFrom(c));
	}
}
