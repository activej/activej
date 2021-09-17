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

/**
 * Utility methods that allow to handle fatal errors using thread-specific handlers
 */
public final class FatalErrorHandlers {
	private static volatile FatalErrorHandler globalFatalErrorHandler = FatalErrorHandler.rethrowOnAnyError();

	private static final ThreadLocal<FatalErrorHandler> CURRENT_HANDLER = new ThreadLocal<>();

	/**
	 * Sets a fatal error handler for a current thread
	 *
	 * @param handler a global fatal error handler
	 */
	public static void setThreadFatalErrorHandler(@Nullable FatalErrorHandler handler) {
		if (handler == null) {
			CURRENT_HANDLER.remove();
		} else {
			CURRENT_HANDLER.set(handler);
		}
	}

	/**
	 * Sets a global fatal error handler. This handler will be used if no other handler
	 * was set for a handling thread using {@link #setThreadFatalErrorHandler(FatalErrorHandler)}
	 *
	 * @param handler a global fatal error handler
	 */
	public static void setGlobalFatalErrorHandler(@NotNull FatalErrorHandler handler) {
		globalFatalErrorHandler = handler;
	}

	/**
	 * Returns a thread fatal error handler. If no thread fatal error handler was set using
	 * {@link #setThreadFatalErrorHandler(FatalErrorHandler)}, a global fatal error handler will
	 * be returned
	 *
	 * @return a thread fatal error handler or a global fatal error handler if thread's handler was not set
	 */
	public static @NotNull FatalErrorHandler getThreadFatalErrorHandler() {
		FatalErrorHandler handler = CURRENT_HANDLER.get();
		return handler != null ? handler : globalFatalErrorHandler;
	}

	/**
	 * Uses a given fatal error handler to handle a received {@link Throwable}
	 * <p>
	 * If an error is a checked exception, no handling will be performed
	 * <p>
	 * An optional context may be passed for debug purposes
	 *
	 * @param fatalErrorHandler a fatal error handler
	 * @param e                 an error to be handled
	 * @param context           an optional context that provides additional debug information
	 * @see #getThreadFatalErrorHandler()
	 */
	public static void handleError(@NotNull FatalErrorHandler fatalErrorHandler, @NotNull Throwable e, @Nullable Object context) {
		if (e instanceof RuntimeException || !(e instanceof Exception)) {
			fatalErrorHandler.handle(e, context);
		}
	}

	/**
	 * Uses current thread's fatal error handler to handle a received {@link Throwable}
	 * <p>
	 * If no error handler is set for the current thread, uses a global fatal error handler
	 * <p>
	 * If an error is a checked exception, no handling will be performed
	 * <p>
	 * An optional context may be passed for debug purposes
	 *
	 * @param e       an error to be handled
	 * @param context an optional context that provides additional debug information
	 * @see #getThreadFatalErrorHandler()
	 */
	public static void handleError(@NotNull Throwable e, @Nullable Object context) {
		if (e instanceof RuntimeException || !(e instanceof Exception)) {
			getThreadFatalErrorHandler().handle(e, context);
		}
	}

	/**
	 * @see #handleError(Throwable, Object)
	 */
	public static void handleError(@NotNull Throwable e) {
		handleError(e, null);
	}

	/**
	 * Uses global fatal error handler to handle a received {@link Throwable}
	 * <p>
	 * If an error is a checked exception, no handling will be performed
	 * <p>
	 * An optional context may be passed for debug purposes
	 *
	 * @param e       an error to be handled
	 * @param context an optional context that provides additional debug information
	 */
	public static void handleErrorWithGlobalHandler(@NotNull Throwable e, @Nullable Object context) {
		if (e instanceof RuntimeException || !(e instanceof Exception)) {
			globalFatalErrorHandler.handle(e, context);
		}
	}

	static void shutdownForcibly() {
		Runtime.getRuntime().halt(1);
	}
}
