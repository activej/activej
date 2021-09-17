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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.function.Predicate;

import static io.activej.common.exception.FatalErrorHandlers.shutdownForcibly;

/**
 * A callback for any fatal (unchecked) exceptions
 * <p>
 * Also contains various default handlers
 */
@FunctionalInterface
public interface FatalErrorHandler {
	/**
	 * Called when an unchecked exception is caught during execution of some task
	 *
	 * @param e       the caught exception
	 * @param context the context in which exception was caught in, possibly {@code null}
	 */
	void handle(@NotNull Throwable e, @Nullable Object context);

	/**
	 * @see #handle(Throwable, Object)
	 */
	default void handle(@NotNull Throwable e) {
		handle(e, null);
	}

	/**
	 * Combines handlers by chaining the next handler after the current one
	 */
	default FatalErrorHandler andThen(FatalErrorHandler nextHandler) {
		return (e, context) -> {
			handle(e, context);
			nextHandler.handle(e, context);
		};
	}

	/**
	 * A fatal error handler that simply ignores all received errors
	 */
	static FatalErrorHandler ignoreAllErrors() {
		return (e, context) -> {};
	}

	/**
	 * A fatal error handler that terminates JVM on any error
	 */
	static FatalErrorHandler exitOnAnyError() {
		return (e, context) -> shutdownForcibly();
	}

	/**
	 * A fatal error handler that terminates JVM on any error that matches
	 * a given predicate
	 *
	 * @param predicate a predicate that tests a received error
	 */
	static FatalErrorHandler exitOnMatchedError(Predicate<Throwable> predicate) {
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
	static FatalErrorHandler exitOnMatchedError(Class<? extends Throwable> cls) {
		return exitOnMatchedError(throwable -> cls.isAssignableFrom(throwable.getClass()));
	}

	/**
	 * A fatal error handler that terminates JVM on any {@link Error}
	 */
	static FatalErrorHandler exitOnJvmError() {
		return exitOnMatchedError(e -> e instanceof Error);
	}

	/**
	 * A fatal error handler that rethrows any error it receives
	 */
	static FatalErrorHandler rethrowOnAnyError() {
		return (e, context) -> propagate(e);
	}

	/**
	 * A fatal error handler that rethrows any error that matches
	 * a given predicate
	 *
	 * @param predicate a predicate that tests a received error
	 */
	static FatalErrorHandler rethrowOnMatchedError(Predicate<Throwable> predicate) {
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
	static FatalErrorHandler rethrowOnMatchedError(Class<? extends Throwable> cls) {
		return rethrowOnMatchedError(throwable -> cls.isAssignableFrom(throwable.getClass()));
	}

	/**
	 * A fatal error handler that logs all errors to an internal {@link Logger}
	 */
	static FatalErrorHandler logging() {
		return logging(LoggerFactory.getLogger(FatalErrorHandler.class));
	}

	/**
	 * A fatal error handler that logs all errors to a given {@link Logger}
	 *
	 * @param logger a logger to log all the received errors
	 */
	static FatalErrorHandler logging(Logger logger) {
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
	static FatalErrorHandler loggingTo(PrintStream stream) {
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
	static FatalErrorHandler loggingToStdOut() {
		return loggingTo(System.out);
	}

	/**
	 * A fatal error handler that logs all errors to a standard error output stream
	 *
	 * @see System#err
	 */
	static FatalErrorHandler loggingToStdErr() {
		return loggingTo(System.err);
	}

	/**
	 * Propagates a throwable. Throws unchecked exceptions as-is.
	 * Checked exceptions are wrapped in a {@link RuntimeException}
	 */
	@Contract("_ -> fail")
	static void propagate(@NotNull Throwable e) {
		if (e instanceof Error) {
			throw (Error) e;
		} else if (e instanceof RuntimeException) {
			throw (RuntimeException) e;
		} else {
			throw new RuntimeException(e);
		}
	}

}
