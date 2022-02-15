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

package io.activej.config.converter;

import io.activej.async.service.EventloopTaskScheduler.Schedule;
import io.activej.common.MemSize;
import io.activej.common.StringFormatUtils;
import io.activej.common.exception.FatalErrorHandler;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.eventloop.net.DatagramSocketSettings;
import io.activej.eventloop.net.ServerSocketSettings;
import io.activej.eventloop.net.SocketSettings;
import io.activej.promise.RetryPolicy;
import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static io.activej.common.Utils.not;
import static io.activej.common.exception.FatalErrorHandler.*;
import static io.activej.eventloop.inspector.ThrottlingController.INITIAL_KEYS_PER_SECOND;
import static io.activej.eventloop.inspector.ThrottlingController.INITIAL_THROTTLING;
import static io.activej.eventloop.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static java.util.Collections.emptyList;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"unused", "WeakerAccess"})
public final class ConfigConverters {

	public static ConfigConverter<LocalDate> ofLocalDate() {
		return SimpleConfigConverter.of(LocalDate::parse, LocalDate::toString);
	}

	public static ConfigConverter<LocalTime> ofLocalTime() {
		return SimpleConfigConverter.of(LocalTime::parse, LocalTime::toString);
	}

	public static ConfigConverter<LocalDateTime> ofLocalDateTime() {
		return SimpleConfigConverter.of(StringFormatUtils::parseLocalDateTime, StringFormatUtils::formatLocalDateTime);
	}

	public static ConfigConverter<Period> ofPeriod() {
		return SimpleConfigConverter.of(StringFormatUtils::parsePeriod, StringFormatUtils::formatPeriod);
	}

	/**
	 * Returns a config converter with days in period
	 */
	public static ConfigConverter<Integer> ofPeriodAsDays() {
		return ofPeriod().transform(Period::getDays, Period::ofDays);
	}

	public static ConfigConverter<Duration> ofDuration() {
		return SimpleConfigConverter.of(StringFormatUtils::parseDuration, StringFormatUtils::formatDuration);
	}

	/**
	 * Returns a config converter with millis in duration
	 */
	public static ConfigConverter<Long> ofDurationAsMillis() {
		return ofDuration().transform(Duration::toMillis, Duration::ofMillis);
	}

	public static ConfigConverter<Instant> ofInstant() {
		return SimpleConfigConverter.of(StringFormatUtils::parseInstant, StringFormatUtils::formatInstant);
	}

	/**
	 * Returns a config converter with epoch millis in instant
	 */
	public static ConfigConverter<Long> ofInstantAsEpochMillis() {
		return ofInstant().transform(Instant::toEpochMilli, Instant::ofEpochMilli);
	}

	public static ConfigConverter<String> ofString() {
		return new ConfigConverter<String>() {
			@Override
			public String get(Config config, String defaultValue) {
				return config.getValue(defaultValue);
			}

			@Override
			public @NotNull String get(Config config) {
				return config.getValue();
			}
		};
	}

	public static ConfigConverter<Byte> ofByte() {
		return SimpleConfigConverter.of(Byte::valueOf, aByte -> Byte.toString(aByte));
	}

	public static ConfigConverter<Integer> ofInteger() {
		return SimpleConfigConverter.of(Integer::valueOf, anInteger -> Integer.toString(anInteger));
	}

	public static ConfigConverter<Long> ofLong() {
		return SimpleConfigConverter.of(Long::valueOf, aLong -> Long.toString(aLong));
	}

	public static ConfigConverter<Float> ofFloat() {
		return SimpleConfigConverter.of(Float::valueOf, aFloat -> Float.toString(aFloat));
	}

	public static ConfigConverter<Double> ofDouble() {
		return SimpleConfigConverter.of(Double::valueOf, aDouble -> Double.toString(aDouble));
	}

	public static ConfigConverter<Boolean> ofBoolean() {
		return SimpleConfigConverter.of(Boolean::valueOf, aBoolean -> Boolean.toString(aBoolean));
	}

	public static <E extends Enum<E>> SimpleConfigConverter<E> ofEnum(Class<E> enumClass) {
		return SimpleConfigConverter.of(string -> Enum.valueOf(enumClass, string), Enum::name);
	}

	public static ConfigConverter<Class<?>> ofClass() {
		return SimpleConfigConverter.of(Class::forName, Class::getName);
	}

	public static ConfigConverter<InetAddress> ofInetAddress() {
		return SimpleConfigConverter.of(InetAddress::getByName, InetAddress::getHostAddress);
	}

	public static ConfigConverter<InetSocketAddress> ofInetSocketAddress() {
		return SimpleConfigConverter.of(
				StringFormatUtils::parseInetSocketAddress,
				value -> value.getAddress().getHostAddress() + ":" + value.getPort()
		);
	}

	public static ConfigConverter<Path> ofPath() {
		return SimpleConfigConverter.of(Paths::get, value -> value.toAbsolutePath().normalize().toString());
	}

	public static ConfigConverter<MemSize> ofMemSize() {
		return SimpleConfigConverter.of(MemSize::valueOf, MemSize::format);
	}

	/**
	 * Returns a config converter with bytes in MemSize
	 */
	public static ConfigConverter<Long> ofMemSizeAsLong() {
		return ofMemSize().transform(MemSize::toLong, MemSize::of);
	}

	/**
	 * Returns a config converter with bytes in MemSize
	 */
	public static ConfigConverter<Integer> ofMemSizeAsInt() {
		return ofMemSize().transform(MemSize::toInt, (Function<Integer, MemSize>) MemSize::of);
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverter<T> elementConverter, CharSequence separators) {
		return new SimpleConfigConverter<List<T>>() {
			private final Pattern pattern = compile(separators.chars()
					.mapToObj(c -> "\\" + (char) c)
					.collect(joining("", "[", "]")));

			@Override
			public List<T> fromString(String string) {
				return pattern.splitAsStream(string)
						.map(String::trim)
						.filter(not(String::isEmpty))
						.map(s -> elementConverter.get(Config.ofValue(s)))
						.collect(toList());
			}

			@Override
			public String toString(List<T> value) {
				return value.stream()
						.map(v -> {
							Config config = Config.ofValue(elementConverter, v);
							if (config.hasChildren()) {
								throw new AssertionError("Unexpected child entries: " + config.toMap());
							}
							return config.getValue();
						})
						.collect(joining(String.valueOf(separators.charAt(0))));
			}
		};
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverter<T> elementConverter) {
		return ofList(elementConverter, ",;");
	}

	// compound
	public static ConfigConverter<ServerSocketSettings> ofServerSocketSettings() {
		return new ComplexConfigConverter<ServerSocketSettings>(ServerSocketSettings.create(DEFAULT_BACKLOG)) {
			@Override
			protected ServerSocketSettings provide(Config config, ServerSocketSettings defaultValue) {
				return Function.<ServerSocketSettings>identity()
						.andThen(apply(
								ServerSocketSettings::withBacklog,
								config.get(ofInteger(), "backlog", defaultValue.getBacklog())))
						.andThen(applyIfNotNull(
								ServerSocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? defaultValue.getReceiveBufferSize() : null)))
						.andThen(applyIfNotNull(
								ServerSocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.apply(ServerSocketSettings.create(DEFAULT_BACKLOG));
			}
		};
	}

	public static ConfigConverter<SocketSettings> ofSocketSettings() {
		return new ComplexConfigConverter<SocketSettings>(SocketSettings.create()) {
			@Override
			protected SocketSettings provide(Config config, SocketSettings defaultValue) {
				return Function.<SocketSettings>identity()
						.andThen(applyIfNotNull(
								SocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? defaultValue.getReceiveBufferSize() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withSendBufferSize,
								config.get(ofMemSize(), "sendBufferSize",
										defaultValue.hasSendBufferSize() ? defaultValue.getSendBufferSize() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withKeepAlive,
								config.get(ofBoolean(), "keepAlive",
										defaultValue.hasKeepAlive() ? defaultValue.getKeepAlive() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withTcpNoDelay,
								config.get(ofBoolean(), "tcpNoDelay",
										defaultValue.hasTcpNoDelay() ? defaultValue.getTcpNoDelay() : null)))
						.andThen(applyIfNotNull(SocketSettings::withLingerTimeout,
								config.get(ofDuration(), "lingerTimeout",
										defaultValue.hasLingerTimeout() ? defaultValue.getLingerTimeout() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withImplReadTimeout,
								config.get(ofDuration(), "implReadTimeout",
										defaultValue.hasImplReadTimeout() ? defaultValue.getImplReadTimeout() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withImplWriteTimeout,
								config.get(ofDuration(), "implWriteTimeout",
										defaultValue.hasImplWriteTimeout() ? defaultValue.getImplWriteTimeout() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withImplReadBufferSize,
								config.get(ofMemSize(), "implReadBufferSize",
										defaultValue.hasReadBufferSize() ? defaultValue.getImplReadBufferSize() : null)))
						.apply(SocketSettings.create());
			}
		};
	}

	public static ConfigConverter<DatagramSocketSettings> ofDatagramSocketSettings() {
		return new ComplexConfigConverter<DatagramSocketSettings>(DatagramSocketSettings.create()) {
			@Override
			protected DatagramSocketSettings provide(Config config, DatagramSocketSettings defaultValue) {
				return Function.<DatagramSocketSettings>identity()
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? defaultValue.getReceiveBufferSize() : null)))
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withSendBufferSize,
								config.get(ofMemSize(), "sendBufferSize",
										defaultValue.hasSendBufferSize() ? defaultValue.getSendBufferSize() : null)))
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withBroadcast,
								config.get(ofBoolean(), "broadcast",
										defaultValue.hasBroadcast() ? defaultValue.getBroadcast() : null)))
						.apply(DatagramSocketSettings.create());
			}
		};
	}

	public static final ConfigConverter<List<Class<?>>> OF_CLASSES = ofList(ofClass());

	public static ConfigConverter<FatalErrorHandler> ofFatalErrorHandler() {
		return new ConfigConverter<FatalErrorHandler>() {
			@Override
			public @NotNull FatalErrorHandler get(Config config) {
				switch (config.getValue()) {
					case "rethrow":
						return rethrow();
					case "ignore":
						return ignore();
					case "halt":
						return halt();
					case "haltOnError":
						return haltOnError();
					case "haltOnVirtualMachineError":
						return haltOnVirtualMachineError();
					case "haltOnOutOfMemoryError":
						return haltOnOutOfMemoryError();
					case "rethrowOn":
						return rethrowOn(
								toThrowablePredicate(
										config.get(OF_CLASSES, "whitelist", emptyList()),
										config.get(OF_CLASSES, "blacklist", emptyList())
								));
					case "haltOn":
						return haltOn(
								toThrowablePredicate(
										config.get(OF_CLASSES, "whitelist", emptyList()),
										config.get(OF_CLASSES, "blacklist", emptyList())
								));
					case "logging":
						return logging();
					case "loggingToSystemOut":
						return loggingToSystemOut();
					case "loggingToSystemErr":
						return loggingToSystemErr();
					case "loggingToEventloop":
						FatalErrorHandler logging = logging();
						return (e, context) -> {
							Eventloop eventloop = Eventloop.getCurrentEventloopOrNull();
							if (eventloop != null) {
								eventloop.logFatalError(e, context);
							} else {
								logging.handle(e, context);
							}
						};
					default:
						throw new IllegalArgumentException("No fatal error handler named " + config.getValue() + " exists!");
				}
			}

			@Override
			public FatalErrorHandler get(Config config, FatalErrorHandler defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}

	private static Predicate<Throwable> toThrowablePredicate(List<Class<?>> whiteList, List<Class<?>> blackList) {
		return e -> matchesAny(e.getClass(), whiteList) && !matchesAny(e.getClass(), blackList);
	}

	private static boolean matchesAny(Class<?> c, List<Class<?>> list) {
		return list.stream().anyMatch(cl -> cl.isAssignableFrom(c));
	}

	public static ConfigConverter<Schedule> ofEventloopTaskSchedule() {
		return new ConfigConverter<Schedule>() {
			@Override
			public @NotNull Schedule get(Config config) {
				switch (config.get("type")) {
					case "immediate":
						return Schedule.immediate();
					case "delay":
						return Schedule.ofDelay(config.get(ofDuration(), "value"));
					case "interval":
						return Schedule.ofInterval(config.get(ofDuration(), "value"));
					case "period":
						return Schedule.ofPeriod(config.get(ofDuration(), "value"));
					default:
						throw new IllegalArgumentException("No eventloop task schedule type named " + config.getValue() + " exists!");
				}
			}

			@Override
			public Schedule get(Config config, Schedule defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}

	@SuppressWarnings("rawtypes")
	public static ConfigConverter<RetryPolicy> ofRetryPolicy() {
		return new ConfigConverter<RetryPolicy>() {
			@Override
			public @NotNull RetryPolicy get(Config config) {
				if (!config.hasValue() || config.getValue().equals("no")) {
					return RetryPolicy.noRetry();
				}
				RetryPolicy retryPolicy;
				switch (config.getValue()) {
					case "immediate":
						retryPolicy = RetryPolicy.immediateRetry();
						break;
					case "fixedDelay":
						retryPolicy = RetryPolicy.fixedDelay(config.get(ofDuration(), "delay").toMillis());
						break;
					case "exponentialBackoff":
						retryPolicy = RetryPolicy.exponentialBackoff(config.get(ofDuration(), "initialDelay").toMillis(),
								config.get(ofDuration(), "maxDelay").toMillis(), config.get(ofDouble(), "exponent", 2.0));
						break;
					default:
						throw new IllegalArgumentException("No retry policy named " + config.getValue() + " exists!");
				}
				int maxRetryCount = config.get(ofInteger(), "maxRetryCount", Integer.MAX_VALUE);
				if (maxRetryCount != Integer.MAX_VALUE) {
					retryPolicy = retryPolicy.withMaxTotalRetryCount(maxRetryCount);
				}
				Duration max = Duration.ofSeconds(Long.MAX_VALUE);
				Duration maxRetryTimeout = config.get(ofDuration(), "maxRetryTimeout", max);
				if (!maxRetryTimeout.equals(max)) {
					retryPolicy = retryPolicy.withMaxTotalRetryTimeout(maxRetryTimeout);
				}
				return retryPolicy;
			}

			@Override
			public RetryPolicy get(Config config, RetryPolicy defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}

	public static ConfigConverter<ThrottlingController> ofThrottlingController() {
		return new ComplexConfigConverter<ThrottlingController>(ThrottlingController.create()) {
			@Override
			protected ThrottlingController provide(Config config, ThrottlingController defaultValue) {
				return ThrottlingController.create()
						.withTargetTime(config.get(ofDuration(), "targetTime", defaultValue.getTargetTime()))
						.withGcTime(config.get(ofDuration(), "gcTime", defaultValue.getGcTime()))
						.withSmoothingWindow(config.get(ofDuration(), "smoothingWindow", defaultValue.getSmoothingWindow()))
						.withThrottlingDecrease(config.get(ofDouble(), "throttlingDecrease", defaultValue.getThrottlingDecrease()))
						.withInitialKeysPerSecond(config.get(ofDouble(), "initialKeysPerSecond", INITIAL_KEYS_PER_SECOND))
						.withInitialThrottling(config.get(ofDouble(), "initialThrottling", INITIAL_THROTTLING));
			}
		};
	}

	/**
	 * Creates {@link ExecutorService} based on given {@link Config}
	 * If config contains no settings related to thread pool, the default executor
	 * will be single threaded with an unbounded task queue
	 *
	 * @param config - configuration of an executor
	 * @return executor service
	 */
	public static ExecutorService getExecutor(Config config) {
		int corePoolSize = config.get(ofInteger().withConstraint(x -> x >= 0), "corePoolSize", 0);
		int maxPoolSize = config.get(ofInteger().withConstraint(x -> x >= corePoolSize), "maxPoolSize", Integer.MAX_VALUE);
		int keepAlive = config.get(ofInteger().withConstraint(x -> x >= 0), "keepAliveSeconds", 60);
		int queueSize = config.get(ofInteger().withConstraint(x -> x >= 0), "queueSize", Integer.MAX_VALUE);

		BlockingQueue<Runnable> queue;
		if (queueSize == 0) {
			queue = new SynchronousQueue<>();
		} else if (queueSize == Integer.MAX_VALUE) {
			queue = new LinkedBlockingQueue<>();
		} else {
			queue = new ArrayBlockingQueue<>(queueSize);
		}

		return new ThreadPoolExecutor(
				corePoolSize,
				maxPoolSize == 0 ? Integer.MAX_VALUE : maxPoolSize,
				keepAlive,
				TimeUnit.SECONDS,
				queue);
	}

	public static ConfigConverter<ExecutorService> ofExecutor() {
		return new ConfigConverter<ExecutorService>() {
			@Override
			public @NotNull ExecutorService get(Config config) {
				return getExecutor(config);
			}

			@Override
			public ExecutorService get(Config config, ExecutorService defaultValue) {
				throw new IllegalArgumentException("Should not use executor config converter with default value");
			}
		};
	}

	public static <T, V> UnaryOperator<T> apply(BiFunction<T, ? super V, T> modifier, V value) {
		return instance -> modifier.apply(instance, value);
	}

	public static <T, V> UnaryOperator<T> applyIf(BiFunction<T, ? super V, T> modifier, V value, Predicate<? super V> predicate) {
		return instance -> {
			if (!predicate.test(value)) return instance;
			return modifier.apply(instance, value);
		};
	}

	public static <T, V> UnaryOperator<T> applyIfNotNull(BiFunction<T, ? super V, T> modifier, V value) {
		return applyIf(modifier, value, Objects::nonNull);
	}

}
