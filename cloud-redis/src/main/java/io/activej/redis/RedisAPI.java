package io.activej.redis;

import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface RedisAPI {
	// region connection
	Promise<String> auth(String password);

	Promise<String> auth(String username, String password);

	Promise<String> clientSetname(String connectionName);

	Promise<@Nullable String> clientGetname();

	Promise<String> clientPause(Duration pauseDuration);

	Promise<String> echo(String message);

	Promise<String> ping();

	Promise<String> ping(String message);

	Promise<Void> quit();

	Promise<String> select(int dbIndex);
	// endregion

	// region keys
	Promise<Long> del(String key, String... otherKeys);

	Promise<byte[]> dump(String key);

	Promise<Long> exists(String key, String... otherKeys);

	Promise<Long> expire(String key, long ttlSeconds);

	Promise<Long> expireat(String key, long unixTimestampSeconds);

	Promise<List<String>> keys(String pattern);

	default Promise<List<String>> keys() {
		return keys("*");
	}

	Promise<Long> move(String key, int dbIndex);

	Promise<Long> persist(String key);

	Promise<Long> pexpire(String key, long ttlMillis);

	default Promise<Long> expire(String key, Duration ttl) {
		return pexpire(key, ttl.toMillis());
	}

	Promise<Long> pexpireat(String key, long unixTimestampMillis);

	default Promise<Long> expireat(String key, Instant expiration) {
		return pexpireat(key, expiration.toEpochMilli());
	}

	Promise<Long> pttl(String key);

	Promise<@Nullable String> randomkey();

	Promise<String> rename(String key, String newKey);

	Promise<String> renamenx(String key, String newKey);

	Promise<Void> restore(String key, Duration ttl, byte[] dump);

	Promise<Long> touch(String key, String... otherKeys);

	Promise<Long> ttl(String key);

	Promise<RedisType> type(String key);

	Promise<Long> unlink(String key, String... otherKeys);

	Promise<Long> wait(int numberOfReplicas, long timeoutMillis);

	default Promise<Long> wait(int numberOfReplicas, Duration timeout) {
		return wait(numberOfReplicas, timeout.toMillis());
	}
	// endregion

	// region strings
	Promise<Long> append(String key, byte[] value);

	Promise<Long> append(String key, String value);

	Promise<Long> bitcount(String key);

	Promise<Long> bitcount(String key, int start, int end);

	Promise<Long> bitop(BitOperator operator, String destKey, String sourceKey, String... otherSourceKeys);

	Promise<Long> bitpos(String key, boolean bitIsSet);

	Promise<Long> bitpos(String key, boolean bitIsSet, int start);

	Promise<Long> bitpos(String key, boolean bitIsSet, int start, int end);

	Promise<Long> decr(String key);

	Promise<Long> decrby(String key, long decrByValue);

	Promise<@Nullable String> get(String key);

	Promise<@Nullable byte[]> getAsBinary(String key);

	Promise<Long> getbit(String key, int offset);

	Promise<String> getrange(String key, int start, int end);

	Promise<byte[]> getrangeAsBinary(String key, int start, int end);

	Promise<@Nullable String> getset(String key, String value);

	Promise<@Nullable byte[]> getset(String key, byte[] value);

	Promise<Long> incr(String key);

	Promise<Long> incrby(String key, long incrByValue);

	Promise<Double> incrbyfloat(String key, double incrByFloatValue);

	Promise<List<@Nullable String>> mget(String key, String... otherKeys);

	Promise<List<@Nullable byte[]>> mgetAsBinary(String key, String... otherKeys);

	Promise<Void> mset(Map<@NotNull String, @NotNull byte[]> entries);

	Promise<Void> mset(String key, String value, String... otherKeysAndValue);

	Promise<Long> msetnx(Map<@NotNull String, @NotNull byte[]> entries);

	Promise<Long> msetnx(String key, String value, String... otherKeysAndValue);

	Promise<String> psetex(String key, long millis, String value);

	Promise<String> psetex(String key, long millis, byte[] value);

	Promise<@Nullable String> set(String key, String value, SetModifier... modifiers);

	Promise<@Nullable String> set(String key, byte[] value, SetModifier... modifiers);
	// endregion
}
