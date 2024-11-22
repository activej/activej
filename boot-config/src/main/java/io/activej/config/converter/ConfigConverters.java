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

import io.activej.async.service.TaskScheduler.Schedule;
import io.activej.common.MemSize;
import io.activej.common.StringFormatUtils;
import io.activej.common.annotation.StaticFactories;
import io.activej.common.exception.FatalErrorHandler;
import io.activej.config.Config;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.promise.RetryPolicy;
import io.activej.reactor.Reactor;
import io.activej.reactor.net.DatagramSocketSettings;
import io.activej.reactor.net.ServerSocketSettings;
import io.activej.reactor.net.SocketSettings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.activej.common.exception.FatalErrorHandlers.*;
import static io.activej.eventloop.inspector.ThrottlingController.INITIAL_KEYS_PER_SECOND;
import static io.activej.eventloop.inspector.ThrottlingController.INITIAL_THROTTLING;
import static java.util.function.Predicate.not;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"unused", "WeakerAccess"})
@StaticFactories(ConfigConverter.class)
public class ConfigConverters {

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
		return new ConfigConverter<>() {
			@Override
			public String get(Config config, String defaultValue) {
				return config.getValue(defaultValue);
			}

			@Override
			public String get(Config config) {
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
			StringFormatUtils::parseInetSocketAddressResolving,
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
		return new SimpleConfigConverter<>() {
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
		return new ComplexConfigConverter<>(ServerSocketSettings.defaultInstance()) {
			@Override
			protected ServerSocketSettings provide(Config config, ServerSocketSettings defaultValue) {
				return ServerSocketSettings.builder()
					.set(
						ServerSocketSettings.Builder::withBacklog,
						config.get(ofInteger(), "backlog", defaultValue.getBacklog()))
					.setIfNotNull(
						ServerSocketSettings.Builder::withReceiveBufferSize,
						config.get(ofMemSize(), "receiveBufferSize", defaultValue.getReceiveBufferSize()))
					.setIfNotNull(
						ServerSocketSettings.Builder::withReuseAddress,
						config.get(ofBoolean(), "reuseAddress", defaultValue.getReuseAddress()))
					.build();
			}
		};
	}

	public static ConfigConverter<SocketSettings> ofSocketSettings() {
		return new ComplexConfigConverter<>(SocketSettings.defaultInstance()) {
			@Override
			protected SocketSettings provide(Config config, SocketSettings defaultValue) {
				return SocketSettings.builder()
					.setIfNotNull(
						SocketSettings.Builder::withReceiveBufferSize,
						config.get(ofMemSize(), "receiveBufferSize", defaultValue.getReceiveBufferSize()))
					.setIfNotNull(
						SocketSettings.Builder::withSendBufferSize,
						config.get(ofMemSize(), "sendBufferSize", defaultValue.getSendBufferSize()))
					.setIfNotNull(
						SocketSettings.Builder::withReuseAddress,
						config.get(ofBoolean(), "reuseAddress", defaultValue.getReuseAddress()))
					.setIfNotNull(
						SocketSettings.Builder::withKeepAlive,
						config.get(ofBoolean(), "keepAlive", defaultValue.getKeepAlive()))
					.setIfNotNull(
						SocketSettings.Builder::withTcpNoDelay,
						config.get(ofBoolean(), "tcpNoDelay", defaultValue.getTcpNoDelay()))
					.setIfNotNull(
						SocketSettings.Builder::withLingerTimeout,
						config.get(ofDuration(), "lingerTimeout", defaultValue.getLingerTimeout()))
					.setIfNotNull(
						SocketSettings.Builder::withImplReadTimeout,
						config.get(ofDuration(), "implReadTimeout", defaultValue.getImplReadTimeout()))
					.setIfNotNull(
						SocketSettings.Builder::withImplWriteTimeout,
						config.get(ofDuration(), "implWriteTimeout", defaultValue.getImplWriteTimeout()))
					.setIfNotNull(
						SocketSettings.Builder::withImplReadBufferSize,
						config.get(ofMemSize(), "implReadBufferSize", defaultValue.getImplReadBufferSize())).build();
			}
		};
	}

	public static ConfigConverter<DatagramSocketSettings> ofDatagramSocketSettings() {
		return new ComplexConfigConverter<>(DatagramSocketSettings.create()) {
			@Override
			protected DatagramSocketSettings provide(Config config, DatagramSocketSettings defaultValue) {
				return DatagramSocketSettings.builder()
					.setIfNotNull(
						DatagramSocketSettings.Builder::withReceiveBufferSize,
						config.get(ofMemSize(), "receiveBufferSize", defaultValue.getReceiveBufferSize()))
					.setIfNotNull(
						DatagramSocketSettings.Builder::withSendBufferSize,
						config.get(ofMemSize(), "sendBufferSize", defaultValue.getSendBufferSize()))
					.setIfNotNull(
						DatagramSocketSettings.Builder::withReuseAddress,
						config.get(ofBoolean(), "reuseAddress", defaultValue.getReuseAddress()))
					.setIfNotNull(
						DatagramSocketSettings.Builder::withBroadcast,
						config.get(ofBoolean(), "broadcast", defaultValue.getBroadcast()))
					.build();
			}
		};
	}

	private static final ConfigConverter<List<Class<?>>> OF_CLASSES = ofList(ofClass());

	public static ConfigConverter<List<Class<?>>> ofClasses() {
		return OF_CLASSES;
	}

	public static ConfigConverter<FatalErrorHandler> ofFatalErrorHandler() {
		return new ConfigConverter<>() {
			@Override
			public FatalErrorHandler get(Config config) {
				return switch (config.getValue()) {
					case "rethrow" -> rethrow();
					case "ignore" -> ignore();
					case "halt" -> halt();
					case "haltOnError" -> haltOnError();
					case "haltOnVirtualMachineError" -> haltOnVirtualMachineError();
					case "haltOnOutOfMemoryError" -> haltOnOutOfMemoryError();
					case "rethrowOn" -> rethrowOn(
						toThrowablePredicate(
							config.get(OF_CLASSES, "whitelist", List.of()),
							config.get(OF_CLASSES, "blacklist", List.of())
						));
					case "haltOn" -> haltOn(
						toThrowablePredicate(
							config.get(OF_CLASSES, "whitelist", List.of()),
							config.get(OF_CLASSES, "blacklist", List.of())
						));
					case "logging" -> logging();
					case "loggingToSystemOut" -> loggingToSystemOut();
					case "loggingToSystemErr" -> loggingToSystemErr();
					case "loggingToEventloop" -> {
						FatalErrorHandler logging = logging();
						yield (e, context) -> {
							Reactor reactor = Reactor.getCurrentReactorOrNull();
							if (reactor != null) {
								reactor.logFatalError(e, context);
							} else {
								logging.handle(e, context);
							}
						};
					}
					default ->
						throw new IllegalArgumentException("No fatal error handler named " + config.getValue() + " exists!");
				};
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

	public static ConfigConverter<Schedule> ofReactorTaskSchedule() {
		return new ConfigConverter<>() {
			@Override
			public Schedule get(Config config) {
				return switch (config.get("type")) {
					case "immediate" -> Schedule.immediate();
					case "delay" -> Schedule.ofDelay(config.get(ofDuration(), "value"));
					case "interval" -> Schedule.ofInterval(config.get(ofDuration(), "value"));
					case "period" -> Schedule.ofPeriod(config.get(ofDuration(), "value"));
					default ->
						throw new IllegalArgumentException("No reactor task schedule type named " + config.getValue() + " exists!");
				};
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
		return new ConfigConverter<>() {
			@Override
			public RetryPolicy get(Config config) {
				if (!config.hasValue() || config.getValue().equals("no")) {
					return RetryPolicy.noRetry();
				}
				RetryPolicy retryPolicy = switch (config.getValue()) {
					case "immediate" -> RetryPolicy.immediateRetry();
					case "fixedDelay" -> RetryPolicy.fixedDelay(config.get(ofDuration(), "delay").toMillis());
					case "exponentialBackoff" ->
						RetryPolicy.exponentialBackoff(config.get(ofDuration(), "initialDelay").toMillis(),
							config.get(ofDuration(), "maxDelay").toMillis(), config.get(ofDouble(), "exponent", 2.0));
					default ->
						throw new IllegalArgumentException("No retry policy named " + config.getValue() + " exists!");
				};
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
		return new ComplexConfigConverter<>(ThrottlingController.create()) {
			@Override
			protected ThrottlingController provide(Config config, ThrottlingController defaultValue) {
				return ThrottlingController.builder()
					.withTargetTime(config.get(ofDuration(), "targetTime", defaultValue.getTargetTime()))
					.withGcTime(config.get(ofDuration(), "gcTime", defaultValue.getGcTime()))
					.withSmoothingWindow(config.get(ofDuration(), "smoothingWindow", defaultValue.getSmoothingWindow()))
					.withThrottlingDecrease(config.get(ofDouble(), "throttlingDecrease", defaultValue.getThrottlingDecrease()))
					.withInitialKeysPerSecond(config.get(ofDouble(), "initialKeysPerSecond", INITIAL_KEYS_PER_SECOND))
					.withInitialThrottling(config.get(ofDouble(), "initialThrottling", INITIAL_THROTTLING))
					.build();
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
		return new ConfigConverter<>() {
			@Override
			public ExecutorService get(Config config) {
				return getExecutor(config);
			}

			@Override
			public ExecutorService get(Config config, ExecutorService defaultValue) {
				throw new IllegalArgumentException("Should not use executor config converter with default value");
			}
		};
	}

}
