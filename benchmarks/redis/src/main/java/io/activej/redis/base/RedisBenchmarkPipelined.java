package io.activej.redis.base;

import io.activej.async.callback.Callback;
import io.activej.redis.RedisConnection;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static java.lang.Math.min;

public abstract class RedisBenchmarkPipelined extends AbstractRedisBenchmark {
	private static final int ACTIVE_REQUESTS_MIN = 10_000;
	private static final int ACTIVE_REQUESTS_MAX = 10_000;

	private int activeRequestsMin;
	private int activeRequestsMax;

	@Override
	protected void onStart() {
		activeRequestsMin = config.get(ofInteger(), "benchmark.activeRequestsMin", ACTIVE_REQUESTS_MIN);
		activeRequestsMax = config.get(ofInteger(), "benchmark.activeRequestsMax", ACTIVE_REQUESTS_MAX);
		super.onStart();
	}

	@Override
	protected void onStart(RedisConnection connection, Callback<Object> cb) {
		for (int i = 0; i < min(activeRequestsMax, totalRequests); i++, sent++) {
			redisCommand(connection).subscribe(cb);
		}
	}

	@Override
	protected void onResponse(RedisConnection connection, Callback<Object> cb, int active) {
		if (active <= activeRequestsMin) {
			for (int i = 0; i < min(activeRequestsMax - active, totalRequests - sent); i++, sent++) {
				redisCommand(connection).subscribe(cb);
			}
		}
	}
}
