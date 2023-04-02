package io.activej.redis.base;

import io.activej.async.callback.Callback;
import io.activej.redis.RedisConnection;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static java.lang.Math.min;

public abstract class RedisBenchmarkPipelinedBatched extends AbstractRedisBenchmark {
	private static final int BATCH_SIZE = 10_000;

	private int batchSize;

	@Override
	protected void onStart() {
		batchSize = config.get(ofInteger(), "benchmark.batchSize", BATCH_SIZE);
		super.onStart();
	}

	@Override
	protected void onStart(RedisConnection connection, Callback<Object> cb) {
		for (int i = 0; i < min(batchSize, totalRequests); i++, sent++) {
			redisCommand(connection).subscribe(cb);
		}
	}

	@Override
	protected void onResponse(RedisConnection connection, Callback<Object> cb, int active) {
		if (active == 0) {
			for (int i = 0; i < min(batchSize, totalRequests - sent); i++, sent++) {
				redisCommand(connection).subscribe(cb);
			}
		}
	}
}
