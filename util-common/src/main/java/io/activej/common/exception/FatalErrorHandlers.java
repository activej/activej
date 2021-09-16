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

import java.io.PrintStream;
import java.util.function.Predicate;

/**
 * Encapsulation of certain fatal error handlers that determine behaviour in case of fatal error
 * occurrences.
 * <p>
 * Also contains a thread specific handlers of fatal errors as well as a global handler
 */
public final class FatalErrorHandlers {
	private static final Logger logger = LoggerFactory.getLogger(FatalErrorHandlers.class);

	private static volatile FatalErrorHandler globalFatalErrorHandler = FatalErrorHandlers.rethrowOnAnyError();

	private static final ThreadLocal<FatalErrorHandler> CURRENT_HANDLER = new ThreadLocal<>();

	/**
	 * Sets a fatal error handler for a current thread
	 *
	 * @param handler a global fatal error handler
	 */
	public static void setThreadFatalErrorHandler(@NotNull FatalErrorHandler handler) {
		CURRENT_HANDLER.set(handler);
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
	 * Uses current thread's fatal error handler to handle a received {@link Throwable}
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

	public static void handleError(@NotNull FatalErrorHandler fatalErrorHandler, @NotNull Throwable e, @Nullable Object context) {
		if (e instanceof RuntimeException || !(e instanceof Exception)) {
			fatalErrorHandler.handle(e, context);
		}
	}

	/**
	 * @see #handleError(Throwable, Object)
	 */
	public static void handleError(@NotNull Throwable e) {
		handleError(e, null);
	}

	/**
	 * A fatal error handler that simply ignores all received errors
	 */
	public static FatalErrorHandler ignoreAllErrors() {
		return (e, context) -> {};
	}

	/**
	 * A fatal error handler that terminates JVM on any error
	 */
	public static FatalErrorHandler exitOnAnyError() {
		return (e, context) -> shutdownForcibly();
	}

	/**
	 * A fatal error handler that terminates JVM on any error that matches
	 * a given predicate
	 *
	 * @param predicate a predicate that tests a received error
	 */
	public static FatalErrorHandler exitOnMatchedError(Predicate<Throwable> predicate) {
		return (e, context) -> {
			if (predicate.test(e)) {
				shutdownForcibly();
			}
		};
	}

	/**
	 * A fatal error handler that terminates JVM on any error that is an instance of
	 * a given class
	 *
	 * @param cls an exception class that error class is tested against
	 */
	public static FatalErrorHandler exitOnMatchedError(Class<? extends Throwable> cls) {
		return exitOnMatchedError(throwable -> cls.isAssignableFrom(throwable.getClass()));
	}

	/**
	 * A fatal error handler that terminates JVM on any {@link Error}
	 */
	public static FatalErrorHandler exitOnJvmError() {
		return exitOnMatchedError(e -> e instanceof Error);
	}

	/**
	 * A fatal error handler that rethrows any error it receives
	 */
	public static FatalErrorHandler rethrowOnAnyError() {
		return (e, context) -> propagate(e);
	}

	/**
	 * A fatal error handler that rethrows any error that matches
	 * a given predicate
	 *
	 * @param predicate a predicate that tests a received error
	 */
	public static FatalErrorHandler rethrowOnMatchedError(Predicate<Throwable> predicate) {
		return (e, context) -> {
			if (predicate.test(e)) {
				propagate(e);
			}
		};
	}

	/**
	 * A fatal error handler that rethrows any error that is an instance of
	 * a given class
	 *
	 * @param cls an exception class that error class is tested against
	 */
	public static FatalErrorHandler rethrowOnMatchedError(Class<? extends Throwable> cls) {
		return rethrowOnMatchedError(throwable -> cls.isAssignableFrom(throwable.getClass()));
	}

	/**
	 * A fatal error handler that logs all errors to an internal {@link #logger}
	 */
	public static FatalErrorHandler logging() {
		return logging(logger);
	}

	/**
	 * A fatal error handler that logs all errors to a given {@link Logger}
	 *
	 * @param logger a logger to log all the received errors
	 */
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

	/**
	 * A fatal error handler that logs all errors to a given {@link PrintStream}
	 *
	 * @param stream a print stream to log all the received errors
	 */
	public static FatalErrorHandler loggingTo(PrintStream stream) {
		return (e, context) -> {
			if (context == null) {
				stream.println("Fatal error");
			} else {
				stream.println("Fatal error in " + context);
			}
			e.printStackTrace(stream);
		};
	}

	/**
	 * A fatal error handler that logs all errors to a standard output stream
	 *
	 * @see System#out
	 */
	public static FatalErrorHandler loggingToStdOut() {
		return loggingTo(System.out);
	}

	/**
	 * A fatal error handler that logs all errors to a standard error output stream
	 *
	 * @see System#err
	 */
	public static FatalErrorHandler loggingToStdErr() {
		return loggingTo(System.err);
	}

	/**
	 * Propagates a throwable. Throws unchecked exceptions as-is.
	 * Checked exceptions are wrapped in a {@link RuntimeException}
	 */
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
