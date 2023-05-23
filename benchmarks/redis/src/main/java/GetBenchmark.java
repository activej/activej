import io.activej.promise.Promise;
import io.activej.redis.RedisConnection;
import io.activej.redis.RedisRequest;
import io.activej.redis.RedisResponse;
import io.activej.redis.base.RedisBenchmarkConsecutive;
import io.activej.redis.base.RedisBenchmarkPipelined;
import io.activej.redis.base.RedisBenchmarkPipelinedBatched;

public final class GetBenchmark {
	static final class Consecutive extends RedisBenchmarkConsecutive {
		@Override
		protected Promise<?> beforeRound(RedisConnection connection) {
			return connection.cmd(RedisRequest.of("SET", key, value), RedisResponse.SKIP);
		}

		@Override
		protected Promise<?> redisCommand(RedisConnection connection) {
			return connection.cmd(RedisRequest.of("GET", key), RedisResponse.BYTES_ISO_8859_1)
				.whenResult(res -> {
					if (!value.equals(res)) {
						throw new AssertionError();
					}
				});
		}

		public static void main(String[] args) throws Exception {
			new Consecutive().launch(args);
		}
	}

	static final class Pipelined extends RedisBenchmarkPipelined {
		@Override
		protected Promise<?> beforeRound(RedisConnection connection) {
			return connection.cmd(RedisRequest.of("SET", key, value), RedisResponse.SKIP);
		}

		@Override
		protected Promise<?> redisCommand(RedisConnection connection) {
			return connection.cmd(RedisRequest.of("GET", key), RedisResponse.BYTES_ISO_8859_1)
				.whenResult(res -> {
					if (!value.equals(res)) {
						throw new AssertionError();
					}
				});
		}

		public static void main(String[] args) throws Exception {
			new Pipelined().launch(args);
		}
	}

	static final class PipelinedBatched extends RedisBenchmarkPipelinedBatched {
		@Override
		protected Promise<?> beforeRound(RedisConnection connection) {
			return connection.cmd(RedisRequest.of("SET", key, value), RedisResponse.SKIP);
		}

		@Override
		protected Promise<?> redisCommand(RedisConnection connection) {
			return connection.cmd(RedisRequest.of("GET", key), RedisResponse.BYTES_ISO_8859_1)
				.whenResult(res -> {
					if (!value.equals(res)) {
						throw new AssertionError();
					}
				});
		}

		public static void main(String[] args) throws Exception {
			new PipelinedBatched().launch(args);
		}
	}
}
