package io.activej.redis.base;

import io.activej.async.callback.Callback;
import io.activej.config.Config;
import io.activej.redis.RedisConnection;

import static io.activej.config.converter.ConfigConverters.ofInteger;

public abstract class RedisBenchmarkConsecutive extends AbstractRedisBenchmark {
	private static final int TOTAL_REQUESTS = 100_000;

	@Override
	protected Config configOverride() {
		return Config.create()
			.with("benchmark.totalRequests", Config.ofValue(ofInteger(), TOTAL_REQUESTS));
	}

	@Override
	protected void onStart(RedisConnection connection, Callback<Object> cb) {
		redisCommand(connection).subscribe(cb);
	}

	@Override
	protected void onResponse(RedisConnection connection, Callback<Object> cb, int active) {
		redisCommand(connection).subscribe(cb);
	}
}
