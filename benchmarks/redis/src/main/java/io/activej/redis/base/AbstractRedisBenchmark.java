package io.activej.redis.base;

import io.activej.async.callback.Callback;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.redis.RedisClient;
import io.activej.redis.RedisConnection;
import io.activej.redis.RedisRequest;
import io.activej.redis.RedisResponse;
import io.activej.service.ServiceGraphModule;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.inject.module.Modules.combine;

public abstract class AbstractRedisBenchmark extends Launcher {
	private static final String TEMPLATE = "_activej_redis_benchmark";

	private static final int TOTAL_REQUESTS = 10_000_000;
	private static final int WARMUP_ROUNDS = 3;
	private static final int BENCHMARK_ROUNDS = 10;
	private static final int KEY_LENGTH = 10;
	private static final int VALUE_LENGTH = 10;

	@Inject
	RedisClient redisClient;

	@Inject
	Eventloop eventloop;

	@Inject
	Config config;

	@Inject
	@Named("Key")
	protected String key;

	@Inject
	@Named("Value")
	protected String value;

	@Provides
	Eventloop eventloopClient() {
		return Eventloop.create()
				.withEventloopFatalErrorHandler(rethrow());
	}

	@Provides
	RedisClient redisClient(Eventloop eventloop, Config config) {
		return RedisClient.create(eventloop, config.get(ofInetSocketAddress(), "redis.address"));
	}

	@Provides
	@Named("Key")
	String key(Config config) {
		return getString(config.get(ofInteger(), "benchmark.keyLength", KEY_LENGTH));
	}

	@Provides
	@Named("Value")
	String value() {
		return getString(config.get(ofInteger(), "benchmark.valueLength", VALUE_LENGTH));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("redis.address", Config.ofValue(ofInetSocketAddress(), RedisClient.DEFAULT_ADDRESS))
				.overrideWith(configOverride())
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger());
	}

	protected int warmupRounds;
	protected int benchmarkRounds;
	protected int totalRequests;

	@Override
	protected void onStart() {
		warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
		totalRequests = config.get(ofInteger(), "benchmark.totalRequests", TOTAL_REQUESTS);
	}

	/**
	 * First counter represents amount of sent requests, so we know when to stop sending them
	 * Second counter represents amount of completed requests(in other words completed will be incremented when
	 * request fails or completes successfully) so we know when to stop round of benchmark
	 */
	protected int sent;
	protected int completed;

	protected abstract void onStart(RedisConnection connection, Callback<Object> cb);

	protected abstract void onResponse(RedisConnection connection, Callback<Object> cb, int active);

	protected abstract Promise<?> redisCommand(RedisConnection connection);

	protected Promise<?> beforeRound(RedisConnection connection) {
		return Promise.complete();
	}

	protected Promise<?> afterRound(RedisConnection connection) {
		return connection.cmd(RedisRequest.of("DEL", key), RedisResponse.SKIP);
	}

	protected Config configOverride() {
		return Config.EMPTY;
	}

	@Override
	protected void run() throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
			long rps = totalRequests * 1000L / roundTime;
			System.out.printf("Round: %d; Round time: %dms; RPS : %d%n", i + 1, roundTime, rps);
		}

		System.out.println("Start benchmarking Redis");

		for (int i = 0; i < benchmarkRounds; i++) {
			long roundTime = round();

			time += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = totalRequests * 1000L / roundTime;
			System.out.printf("Round: %d; Round time: %dms; RPS : %d%n", i + 1, roundTime, rps);
		}
		double avgTime = (double) time / benchmarkRounds;
		long requestsPerSecond = (long) (totalRequests / avgTime * 1000);
		System.out.printf("Time: %dms; Average time: %sms; Best time: %dms; Worst time: %dms; Requests per second: %d%n",
				time, avgTime, bestTime, worstTime, requestsPerSecond);
	}

	private long round() throws Exception {
		return eventloop.submit(this::roundCall).get();
	}

	private Promise<Long> roundCall() {
		return redisClient.connect()
				.then(connection -> beforeRound(connection)
						.then(() -> {
							SettablePromise<Long> promise = new SettablePromise<>();

							long start = System.currentTimeMillis();

							sent = 0;
							completed = 0;

							Callback<Object> callback = new Callback<>() {
								@Override
								public void accept(Object $, @Nullable Exception e) {
									if (e != null) {
										promise.setException(e);
										return;
									}
									completed++;

									int active = sent - completed;

									// Stop round
									if (completed == totalRequests) {
										long elapsed = System.currentTimeMillis() - start;
										afterRound(connection)
												.whenComplete(connection::close)
												.whenResult(() -> promise.set(elapsed))
												.whenException(promise::setException);
										return;
									}

									onResponse(connection, this, active);
								}
							};

							onStart(connection, callback);

							return promise;
						}));
	}

	private static String getString(int length) {
		int templateLength = TEMPLATE.length();
		if (length <= templateLength) return TEMPLATE.substring(0, length);

		StringBuilder stringBuilder = new StringBuilder();
		int full = length / templateLength;
		for (int i = 0; i < full; i++) {
			stringBuilder.append(TEMPLATE);
			length -= templateLength;
		}
		stringBuilder.append(TEMPLATE, 0, length);
		return stringBuilder.toString();
	}
}
