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

package io.activej.async.util;

import io.activej.common.Utils;
import io.activej.common.function.BiConsumerEx;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.async.util.LogUtils.Level.INFO;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static java.util.stream.Collectors.joining;

public class LogUtils {

	public enum Level {
		OFF(null) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return false;
			}
		},

		TRACE(Logger::trace) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isTraceEnabled();
			}
		},

		DEBUG(Logger::debug) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isDebugEnabled();
			}
		},

		INFO(Logger::info) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isInfoEnabled();
			}
		},

		WARN(Logger::warn) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isWarnEnabled();
			}
		},

		ERROR(Logger::error) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isErrorEnabled();
			}
		};

		private final BiConsumer<Logger, String> logConsumer;

		Level(BiConsumer<Logger, String> logConsumer) {
			this.logConsumer = logConsumer;
		}

		protected abstract boolean isEnabled(Logger logger);

		public final void log(Logger logger, Supplier<String> messageSupplier) {
			if (isEnabled(logger)) {
				logConsumer.accept(logger, messageSupplier.get());
			}
		}

		public final void log(Logger logger, String message) {
			if (isEnabled(logger)) {
				logConsumer.accept(logger, message);
			}
		}
	}

	public static String thisMethod() {
		try {
			StackTraceElement stackTraceElement = Thread.currentThread().getStackTrace()[2];
			return stackTraceElement.getMethodName();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> BiConsumerEx<T, Exception> toLogger(Logger logger,
			Level callLevel, Supplier<String> callMsg,
			Level resultLevel, Function<T, String> resultMsg,
			@Nullable Level errorLevel, Function<Exception, String> errorMsg) {
		if (!logger.isErrorEnabled()) return ($, e) -> {};
		callLevel.log(logger, callMsg);
		return (result, e) -> {
			if (e == null) {
				resultLevel.log(logger, () -> resultMsg.apply(result));
			} else {
				if (errorLevel == null) {
					if (logger.isErrorEnabled()) {
						logger.error(errorMsg.apply(e), e);
					}
				} else {
					errorLevel.log(logger, () -> errorMsg.apply(e));
				}
			}
		};
	}

	public static <T> BiConsumerEx<T, Exception> toLogger(Logger logger,
			Level callLevel, Supplier<String> callMsg,
			Level resultLevel, Function<T, String> resultMsg) {
		return toLogger(logger,
				callLevel, callMsg,
				resultLevel, resultMsg,
				null, e -> callMsg.get());
	}

	public static <T> BiConsumerEx<T, Exception> toLogger(Logger logger,
			Level callLevel, Level resultLevel, Level errorLevel,
			String methodName, Object... parameters) {
		return toLogger(logger,
				callLevel, () -> formatCall(methodName, parameters),
				resultLevel, result -> formatResult(methodName, result, parameters),
				errorLevel, errorLevel == null ?
						e -> formatCall(methodName, parameters) :
						e -> formatResult(methodName, e, parameters));
	}

	public static <T> BiConsumerEx<T, Exception> toLogger(Logger logger,
			Level callLevel, Level resultLevel,
			String methodName, Object... parameters) {
		return toLogger(logger, callLevel, resultLevel, null, methodName, parameters);
	}

	public static <T> BiConsumerEx<T, Exception> toLogger(Logger logger,
			Level level,
			String methodName, Object... parameters) {
		return toLogger(logger, level, level, methodName, parameters);
	}

	public static <T> BiConsumerEx<T, Exception> toLogger(Logger logger, String methodName, Object... parameters) {
		return toLogger(logger, TRACE, INFO, methodName, parameters);
	}

	private static String toString(Object object) {
		if (object == null) {
			return "null";
		}
		if (object instanceof Collection) {
			return Utils.toString((Collection<?>) object);
		}
		return object.toString();
	}

	public static String formatCall(String methodName, Object... parameters) {
		return methodName +
				(parameters.length != 0 ? " " + Arrays.stream(parameters)
						.map(LogUtils::toString)
						.collect(joining(", ")) : "") +
				" …";
	}

	public static String formatResult(String methodName, Object result, Object... parameters) {
		return methodName +
				(parameters.length != 0 ? " " + Arrays.stream(parameters)
						.map(LogUtils::toString)
						.collect(joining(", ")) : "") +
				" → " + toString(result);
	}

}
