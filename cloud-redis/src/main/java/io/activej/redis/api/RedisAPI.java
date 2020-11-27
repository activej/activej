package io.activej.redis.api;

import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.activej.common.collection.CollectionUtils.map;

public interface RedisAPI {
	ExpectedRedisException TRANSACTION_FAILED = new ExpectedRedisException("Transaction failed");
	ExpectedRedisException TRANSACTION_DISCARDED = new ExpectedRedisException("Transaction discarded");
	ExpectedRedisException QUIT_CALLED = new ExpectedRedisException("Transaction discarded because QUIT was called");

	// region connection
	Promise<Void> auth(String password);

	Promise<Void> auth(String username, String password);

	Promise<Void> clientSetname(String connectionName);

	Promise<@Nullable String> clientGetname();

	Promise<Void> clientPause(long pauseMillis);

	default Promise<Void> clientPause(Duration pauseDuration) {
		return clientPause(pauseDuration.toMillis());
	}

	Promise<String> echo(String message);

	Promise<String> ping();

	Promise<String> ping(String message);

	Promise<Void> quit();

	Promise<Void> select(int dbIndex);
	// endregion

	// region server
	Promise<Void> flushAll(boolean async);
	// endregion

	// region keys
	Promise<Long> del(String key, String... otherKeys);

	Promise<byte[]> dump(String key);

	Promise<Boolean> exists(String key);

	Promise<Long> exists(String firstKey, String secondKey, String... otherKeys);

	Promise<Boolean> expire(String key, long ttlSeconds);

	Promise<Boolean> expireat(String key, long unixTimestampSeconds);

	Promise<Set<String>> keys(String pattern);

	default Promise<Set<String>> keys() {
		return keys("*");
	}

	Promise<Boolean> migrate(String host, int port, String key, int destinationDb, long timeoutMillis, MigrateModifier... modifiers);

	default Promise<Boolean> migrate(String host, int port, String key, int destinationDb, Duration timeout, MigrateModifier... modifiers) {
		return migrate(host, port, key, destinationDb, timeout.toMillis(), modifiers);
	}

	Promise<Boolean> migrate(String host, int port, int destinationDb, long timeoutMillis, MigrateModifier... modifiers);

	default Promise<Boolean> migrate(String host, int port, int destinationDb, Duration timeout, MigrateModifier... modifiers) {
		return migrate(host, port, destinationDb, timeout.toMillis(), modifiers);
	}

	Promise<Boolean> move(String key, int dbIndex);

	Promise<@Nullable RedisEncoding> objectEncoding(String key);

	Promise<@Nullable Long> objectFreq(String key);

	Promise<String> objectHelp();

	Promise<@Nullable Long> objectIdletime(String key);

	Promise<@Nullable Long> objectRefcount(String key);

	Promise<Boolean> persist(String key);

	Promise<Boolean> pexpire(String key, long ttlMillis);

	default Promise<Boolean> expire(String key, Duration ttl) {
		return pexpire(key, ttl.toMillis());
	}

	Promise<Boolean> pexpireat(String key, long unixTimestampMillis);

	default Promise<Boolean> expireat(String key, Instant expiration) {
		return pexpireat(key, expiration.toEpochMilli());
	}

	Promise<Long> pttl(String key);

	Promise<@Nullable String> randomkey();

	Promise<Void> rename(String key, String newKey);

	Promise<Boolean> renamenx(String key, String newKey);

	Promise<Void> restore(String key, long ttlMillis, byte[] dump, RestoreModifier... modifiers);

	Promise<ScanResult> scan(String cursor, ScanModifier... modifiers);

	Promise<ScanResult> scan(String cursor, RedisType type,  ScanModifier... modifiers);

	ChannelSupplier<String> scanStream(ScanModifier...modifiers);

	ChannelSupplier<byte[]> scanStreamAsBinary(ScanModifier... modifiers);

	ChannelSupplier<String> scanStream(RedisType type, ScanModifier... modifiers);

	ChannelSupplier<byte[]> scanStreamAsBinary(RedisType type, ScanModifier... modifiers);

	default Promise<Void> restore(String key, Duration ttl, byte[] dump, RestoreModifier... modifiers) {
		return restore(key, ttl.toMillis(), dump, modifiers);
	}

	default Promise<Void> restore(String key, byte[] dump, RestoreModifier... modifiers) {
		return restore(key, 0, dump, modifiers);
	}

	Promise<List<String>> sort(String key, SortModifier... modifiers);

	Promise<List<byte[]>> sortAsBinary(String key, SortModifier... modifiers);

	Promise<Long> sort(String key, String destination, SortModifier... modifiers);

	Promise<Long> touch(String key, String... otherKeys);

	Promise<Long> ttl(String key);

	Promise<RedisType> type(String key);

	Promise<Long> unlink(String key, String... otherKeys);

	Promise<Long> wait(int numberOfReplicas, long timeoutMillis);

	default Promise<Long> wait(int numberOfReplicas, Duration timeout) {
		return wait(numberOfReplicas, timeout.toMillis());
	}

	default Promise<Long> wait(int numberOfReplicas) {
		return wait(numberOfReplicas, 0L);
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

	Promise<Boolean> getbit(String key, int offset);

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

	Promise<Void> mset(String key, String value, String... otherKeysAndValues);

	Promise<Long> msetnx(Map<@NotNull String, @NotNull byte[]> entries);

	Promise<Long> msetnx(String key, String value, String... otherKeysAndValues);

	Promise<Void> psetex(String key, long millis, String value);

	Promise<Void> psetex(String key, long millis, byte[] value);

	Promise<@Nullable String> set(String key, String value, SetModifier... modifiers);

	Promise<@Nullable String> set(String key, byte[] value, SetModifier... modifiers);

	Promise<Boolean> setbit(String key, int offset, boolean set);

	Promise<Void> setex(String key, long seconds, String value);

	Promise<Void> setex(String key, long seconds, byte[] value);

	default Promise<Void> setex(String key, Duration ttl, String value) {
		return psetex(key, ttl.toMillis(), value);
	}

	default Promise<Void> setex(String key, Duration ttl, byte[] value) {
		return psetex(key, ttl.toMillis(), value);
	}

	Promise<Boolean> setnx(String key, String value);

	Promise<Boolean> setnx(String key, byte[] value);

	Promise<Long> setrange(String key, int offset, String value);

	Promise<Long> setrange(String key, int offset, byte[] value);

	Promise<Long> strlen(String key);
	// endregion

	// region lists
	Promise<@Nullable ListPopResult> blpop(double timeoutSeconds, String key, String... otherKeys);

	default Promise<@Nullable ListPopResult> blpop(Duration timeout, String key, String... otherKeys) {
		return blpop((double) timeout.toMillis() / 1000, key, otherKeys);
	}

	default Promise<@Nullable ListPopResult> blpop(String key, String... otherKeys) {
		return blpop(0, key, otherKeys);
	}

	Promise<@Nullable ListPopResult> brpop(double timeoutSeconds, String key, String... otherKeys);

	default Promise<@Nullable ListPopResult> brpop(Duration timeout, String key, String... otherKeys) {
		return brpop((double) timeout.toMillis() / 1000, key, otherKeys);
	}

	default Promise<@Nullable ListPopResult> brpop(String key, String... otherKeys) {
		return brpop(0, key, otherKeys);
	}

	Promise<@Nullable String> brpoplpush(String source, String target, double timeoutSeconds);

	default Promise<@Nullable String> brpoplpush(String source, String target, Duration timeout) {
		return brpoplpush(source, target, (double) timeout.toMillis() / 1000);
	}

	default Promise<@Nullable String> brpoplpush(String source, String target) {
		return brpoplpush(source, target, 0);
	}

	Promise<@Nullable byte[]> brpoplpushAsBinary(String source, String target, double timeoutSeconds);

	default Promise<@Nullable byte[]> brpoplpushAsBinary(String source, String target, Duration timeout) {
		return brpoplpushAsBinary(source, target, (double) timeout.toMillis() / 1000);
	}

	default Promise<@Nullable byte[]> brpoplpushAsBinary(String source, String target) {
		return brpoplpushAsBinary(source, target, 0);
	}

	Promise<@Nullable String> lindex(String key, long index);

	Promise<@Nullable byte[]> lindexAsBinary(String key, long index);

	Promise<Long> linsert(String key, InsertPosition position, String pivot, String element);

	Promise<Long> linsert(String key, InsertPosition position, byte[] pivot, byte[] element);

	Promise<Long> llen(String key);

	Promise<@Nullable String> lpop(String key);

	Promise<@Nullable byte[]> lpopAsBinary(String key);

	Promise<@Nullable Long> lpos(String key, String element, LposModifier... modifiers);

	Promise<@Nullable Long> lpos(String key, byte[] element, LposModifier... modifiers);

	Promise<List<Long>> lpos(String key, String element, int count, LposModifier... modifiers);

	Promise<List<Long>> lpos(String key, byte[] element, int count, LposModifier... modifiers);

	Promise<Long> lpush(String key, String element, String... otherElements);

	Promise<Long> lpush(String key, byte[] element, byte[]... otherElements);

	Promise<Long> lpushx(String key, String element, String... otherElements);

	Promise<Long> lpushx(String key, byte[] element, byte[]... otherElements);

	Promise<List<String>> lrange(String key, long start, long stop);

	default Promise<List<String>> lrange(String key, int start) {
		return lrange(key, start, -1);
	}

	default Promise<List<String>> lrange(String key) {
		return lrange(key, 0, -1);
	}

	Promise<List<byte[]>> lrangeAsBinary(String key, long start, long stop);

	default Promise<List<byte[]>> lrangeAsBinary(String key, long start) {
		return lrangeAsBinary(key, start, -1);
	}

	default Promise<List<byte[]>> lrangeAsBinary(String key) {
		return lrangeAsBinary(key, 0, -1);
	}

	Promise<Long> lrem(String key, long count, String element);

	Promise<Long> lrem(String key, long count, byte[] element);

	Promise<Void> lset(String key, long index, String element);

	Promise<Void> lset(String key, long index, byte[] element);

	Promise<Void> ltrim(String key, long start, long stop);

	Promise<@Nullable String> rpop(String key);

	Promise<@Nullable byte[]> rpopAsBinary(String key);

	Promise<@Nullable String> rpoplpush(String source, String destination);

	Promise<@Nullable byte[]> rpoplpushAsBinary(String source, String destination);

	Promise<Long> rpush(String key, String element, String... otherElements);

	Promise<Long> rpush(String key, byte[] element, byte[]... otherElements);

	Promise<Long> rpushx(String key, String element, String... otherElements);

	Promise<Long> rpushx(String key, byte[] element, byte[]... otherElements);
	// endregion

	// region sets
	Promise<Long> sadd(String key, String member, String... otherMembers);

	Promise<Long> sadd(String key, byte[] member, byte[]... otherMembers);

	Promise<Long> scard(String key);

	Promise<Set<String>> sdiff(String key, String... otherKeys);

	Promise<Set<byte[]>> sdiffAsBinary(String key, String... otherKeys);

	Promise<Long> sdiffstore(String destination, String key, String... otherKeys);

	Promise<Set<String>> sinter(String key, String... otherKeys);

	Promise<Set<byte[]>> sinterAsBinary(String key, String... otherKeys);

	Promise<Long> sinterstore(String destination, String key, String... otherKeys);

	Promise<Boolean> sismember(String key, String member);

	Promise<Boolean> sismember(String key, byte[] member);

	Promise<Set<String>> smembers(String key);

	Promise<List<byte[]>> smembersAsBinary(String key);

	Promise<Boolean> smove(String source, String destination, String member);

	Promise<Boolean> smove(String source, String destination, byte[] member);

	Promise<@Nullable String> spop(String key);

	Promise<@Nullable byte[]> spopAsBinary(String key);

	Promise<Set<String>> spop(String key, long count);

	Promise<Set<byte[]>> spopAsBinary(String key, long count);

	Promise<@Nullable String> srandmember(String key);

	Promise<@Nullable byte[]> srandmemberAsBinary(String key);

	Promise<List<String>> srandmember(String key, long count);

	Promise<List<byte[]>> srandmemberAsBinary(String key, long count);

	Promise<Long> srem(String key, String member, String... otherMembers);

	Promise<Long> srem(String key, byte[] member, byte[]... otherMembers);

	Promise<ScanResult> sscan(String key, String cursor, ScanModifier... modifiers);

	ChannelSupplier<String> sscanStream(String key, ScanModifier... modifiers);

	ChannelSupplier<byte[]> sscanStreamAsBinary(String key, ScanModifier... modifiers);

	Promise<Set<String>> sunion(String key, String... otherKeys);

	Promise<Set<byte[]>> sunionAsBinary(String key, String... otherKeys);

	Promise<Long> sunionstore(String destination, String key, String... otherKeys);
	// endregion

	// region hashes
	Promise<Long> hdel(String key, String field, String... otherFields);

	Promise<Boolean> hexists(String key, String field);

	Promise<@Nullable String> hget(String key, String field);

	Promise<@Nullable byte[]> hgetAsBinary(String key, String field);

	Promise<Map<String, String>> hgetall(String key);

	Promise<Map<String, byte[]>> hgetallAsBinary(String key);

	Promise<Long> hincrby(String key, String field, long incrByValue);

	Promise<Double> hincrbyfloat(String key, String field, double incrByValue);

	Promise<Set<String>> hkeys(String key);

	Promise<Long> hlen(String key);

	Promise<List<@Nullable String>> hmget(String key, String field, String... otherFields);

	Promise<List<@Nullable byte[]>> hmgetAsBinary(String key, String field, String... otherFields);

	Promise<Void> hmset(String key, Map<@NotNull String, @NotNull byte[]> entries);

	Promise<Void> hmset(String key, String field, String value, String... otherFieldsAndValues);

	Promise<ScanResult> hscan(String key, String cursor, ScanModifier... modifiers);

	ChannelSupplier<Map.Entry<String, String>> hscanStream(String key, ScanModifier... modifiers);

	ChannelSupplier<Map.Entry<String, byte[]>> hscanStreamAsBinary(String key, ScanModifier... modifiers);

	Promise<Long> hset(String key, Map<@NotNull String, @NotNull byte[]> entries);

	Promise<Long> hset(String key, String field, String value, String... otherFieldsAndValues);

	Promise<Boolean> hsetnx(String key, String field, String value);

	Promise<Boolean> hsetnx(String key, String field, byte[] value);

	Promise<Long> hstrlen(String key, String field);

	Promise<List<String>> hvals(String key);

	Promise<List<byte[]>> hvalsAsBinary(String key);
	// endregion

	// region sorted sets
	Promise<@Nullable SetBlockingPopResult> bzpopmin(double timeoutSeconds, String key, String... otherKeys);

	default Promise<@Nullable SetBlockingPopResult> bzpopmin(Duration timeout, String key, String... otherKeys) {
		return bzpopmin((double) timeout.toMillis() / 1000, key, otherKeys);
	}

	default Promise<@Nullable SetBlockingPopResult> bzpopmin(String key, String... otherKeys) {
		return bzpopmin(0, key, otherKeys);
	}

	Promise<@Nullable SetBlockingPopResult> bzpopmax(double timeoutSeconds, String key, String... otherKeys);

	default Promise<@Nullable SetBlockingPopResult> bzpopmax(Duration timeout, String key, String... otherKeys) {
		return bzpopmax((double) timeout.toMillis() / 1000, key, otherKeys);
	}

	default Promise<@Nullable SetBlockingPopResult> bzpopmax(String key, String... otherKeys) {
		return bzpopmax(0, key, otherKeys);
	}

	Promise<Long> zadd(String key, Map<String, Double> entries, ZaddModifier... modifiers);

	default Promise<Long> zadd(String key, double score, String value, ZaddModifier... modifiers) {
		return zadd(key, map(value, score), modifiers);
	}

	Promise<Double> zaddIncr(String key, double score, String value, ZaddModifier... modifiers);

	Promise<Long> zaddBinary(String key, Map<byte[], Double> entries, ZaddModifier... modifiers);

	default Promise<Long> zadd(String key, double score, byte[] value, ZaddModifier... modifiers) {
		return zaddBinary(key, map(value, score), modifiers);
	}

	Promise<Double> zaddIncr(String key, double score, byte[] value, ZaddModifier... modifiers);

	Promise<Long> zcard(String key);

	Promise<Long> zcount(String key, ScoreInterval scoreInterval);

	Promise<Double> zincrby(String key, double increment, String member);

	Promise<Double> zincrby(String key, double increment, byte[] member);

	Promise<Long> zinterstore(String destination, Aggregate aggregate, Map<String, Double> entries);

	Promise<Long> zinterstore(String destination, Map<String, Double> entries);

	Promise<Long> zinterstore(String destination, Aggregate aggregate, String key, String... otherKeys);

	Promise<Long> zinterstore(String destination, String key, String... otherKeys);

	Promise<Long> zlexcount(String key, LexInterval interval);

	Promise<@Nullable List<SetPopResult>> zpopmax(String key, long count);

	default Promise<@Nullable List<SetPopResult>> zpopmax(String key) {
		return zpopmax(key, 1);
	}

	Promise<@Nullable List<SetPopResult>> zpopmin(String key, long count);

	default Promise<@Nullable List<SetPopResult>> zpopmin(String key) {
		return zpopmin(key, 1);
	}

	Promise<List<String>> zrange(String key, long start, long stop);

	default Promise<List<String>> zrange(String key) {
		return zrange(key, 0, -1);
	}

	Promise<List<byte[]>> zrangeAsBinary(String key, long start, long stop);

	default Promise<List<byte[]>> zrangeAsBinary(String key) {
		return zrangeAsBinary(key, 0, -1);
	}

	Promise<Map<String, Double>> zrangeWithScores(String key, long start, long stop);

	default Promise<Map<String, Double>> zrangeWithScores(String key) {
		return zrangeWithScores(key, 0, -1);
	}

	Promise<Map<byte[], Double>> zrangeAsBinaryWithScores(String key, long start, long stop);

	default Promise<Map<byte[], Double>> zrangeAsBinaryWithScores(String key) {
		return zrangeAsBinaryWithScores(key, 0, -1);
	}

	Promise<List<String>> zrangebylex(String key, LexInterval interval, long offset, long count);

	Promise<List<byte[]>> zrangebylexAsBinary(String key, LexInterval interval, long offset, long count);

	Promise<List<String>> zrangebylex(String key, LexInterval interval);

	Promise<List<byte[]>> zrangebylexAsBinary(String key, LexInterval interval);

	Promise<List<String>> zrevrangebylex(String key, LexInterval interval, long offset, long count);

	Promise<List<byte[]>> zrevrangebylexAsBinary(String key, LexInterval interval, long offset, long count);

	Promise<List<String>> zrevrangebylex(String key, LexInterval interval);

	Promise<List<byte[]>> zrevrangebylexAsBinary(String key, LexInterval interval);

	Promise<List<String>> zrangebyscore(String key, ScoreInterval interval, long offset, long count);

	Promise<List<String>> zrangebyscore(String key, ScoreInterval interval);

	Promise<List<byte[]>> zrangebyscoreAsBinary(String key, ScoreInterval interval, long offset, long count);

	Promise<List<byte[]>> zrangebyscoreAsBinary(String key, ScoreInterval interval);

	Promise<Map<String, Double>> zrangebyscoreWithScores(String key, ScoreInterval interval, long offset, long count);

	Promise<Map<String, Double>> zrangebyscoreWithScores(String key, ScoreInterval interval);

	Promise<Map<byte[], Double>> zrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval, long offset, long count);

	Promise<Map<byte[], Double>> zrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval);

	Promise<@Nullable Long> zrank(String key, String member);

	Promise<@Nullable Long> zrank(String key, byte[] member);

	Promise<Long> zrem(String key, String member, String... otherMembers);

	Promise<Long> zrem(String key, byte[] member, byte[]... otherMembers);

	Promise<Long> zremrangebylex(String key, LexInterval interval);

	Promise<Long> zremrangebyrank(String key, long start, long stop);

	Promise<Long> zremrangebyscore(String key, ScoreInterval interval);

	Promise<List<String>> zrevrange(String key, long start, long stop);

	default Promise<List<String>> zrevrange(String key) {
		return zrevrange(key, 0, -1);
	}

	Promise<List<byte[]>> zrevrangeAsBinary(String key, long start, long stop);

	default Promise<List<byte[]>> zrevrangeAsBinary(String key) {
		return zrevrangeAsBinary(key, 0, -1);
	}

	Promise<Map<String, Double>> zrevrangeWithScores(String key, long start, long stop);

	default Promise<Map<String, Double>> zrevrangeWithScores(String key) {
		return zrevrangeWithScores(key, 0, -1);
	}

	Promise<Map<byte[], Double>> zrevrangeAsBinaryWithScores(String key, long start, long stop);

	default Promise<Map<byte[], Double>> zrevrangeAsBinaryWithScores(String key) {
		return zrangeAsBinaryWithScores(key, 0, -1);
	}

	Promise<List<String>> zrevrangebyscore(String key, ScoreInterval interval);

	Promise<List<byte[]>> zrevrangebyscoreAsBinary(String key, ScoreInterval interval, long offset, long count);

	Promise<List<byte[]>> zrevrangebyscoreAsBinary(String key, ScoreInterval interval);

	Promise<Map<String, Double>> zrevrangebyscoreWithScores(String key, ScoreInterval interval, long offset, long count);

	Promise<Map<String, Double>> zrevrangebyscoreWithScores(String key, ScoreInterval interval);

	Promise<Map<byte[], Double>> zrevrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval, long offset, long count);

	Promise<Map<byte[], Double>> zrevrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval);

	Promise<@Nullable Long> zrevrank(String key, String member);

	Promise<@Nullable Long> zrevrank(String key, byte[] member);

	Promise<ScanResult> zscan(String key, String cursor, ScanModifier... modifiers);

	ChannelSupplier<Map.Entry<String, Double>> zscanStream(String key, ScanModifier... modifiers);

	ChannelSupplier<Map.Entry<byte[], Double>> zscanStreamAsBinary(String key, ScanModifier... modifiers);

	Promise<@Nullable Double> zscore(String key, String member);

	Promise<@Nullable Double> zscore(String key, byte[] member);

	Promise<Long> zunionstore(String destination, Aggregate aggregate, Map<String, Double> entries);

	Promise<Long> zunionstore(String destination, Map<String, Double> entries);

	Promise<Long> zunionstore(String destination, Aggregate aggregate, String key, String... otherKeys);

	Promise<Long> zunionstore(String destination, String key, String... otherKeys);
	// endregion

	// region hyperloglog
	Promise<Long> pfadd(String key, String element, String... otherElements);

	Promise<Long> pfadd(String key, byte[] element, byte[]... otherElements);

	Promise<Long> pfcount(String key, String... otherKeys);

	Promise<Void> pfmerge(String destKey, String sourceKey, String... otherSourceKeys);
	// endregion

	// region geo
	Promise<Long> geoadd(String key, double longitude, double latitude, String member);

	Promise<Long> geoadd(String key, double longitude, double latitude, byte[] member);

	Promise<Long> geoadd(String key, Map<String, Coordinate> coordinates);

	Promise<Long> geoaddBinary(String key, Map<byte[], Coordinate> coordinates);

	Promise<List<String>> geohash(String key, String member, String... otherMembers);

	Promise<List<String>> geohash(String key, byte[] member, byte[]... otherMembers);

	Promise<List<@Nullable Coordinate>> geopos(String key, String member, String... otherMembers);

	Promise<List<@Nullable Coordinate>> geopos(String key, byte[] member, byte[]... otherMembers);

	Promise<@Nullable Double> geodist(String key, String member1, String member2, DistanceUnit unit);

	Promise<@Nullable Double> geodist(String key, String member1, String member2);

	Promise<@Nullable Double> geodist(String key, byte[] member1, byte[] member2, DistanceUnit unit);

	Promise<@Nullable Double> geodist(String key, byte[] member1, byte[] member2);

	Promise<Long> georadius(String key, Coordinate coordinate, double radius, DistanceUnit unit, GeoradiusModifier... modifiers);

	default Promise<Long> georadius(String key, double longitude, double latitude, double radius, DistanceUnit unit, GeoradiusModifier... modifiers) {
		return georadius(key, new Coordinate(longitude, latitude), radius, unit, modifiers);
	}

	Promise<List<GeoradiusResult>> georadiusReadOnly(String key, Coordinate coordinate, double radius, DistanceUnit unit, GeoradiusModifier... modifiers);

	default Promise<List<GeoradiusResult>> georadiusReadOnly(String key, double longitude, double latitude, double radius, DistanceUnit unit, GeoradiusModifier... modifiers) {
		return georadiusReadOnly(key, new Coordinate(longitude, latitude), radius, unit, modifiers);
	}

	Promise<Long> georadiusbymember(String key, String member, double radius, DistanceUnit unit, GeoradiusModifier... modifiers);

	Promise<List<GeoradiusResult>> georadiusbymemberReadOnly(String key, String member, double radius, DistanceUnit unit, GeoradiusModifier... modifiers);
	// endregion

	// region transactions
	Promise<Void> discard();

	Promise<@Nullable List<Object>> exec();

	Promise<Void> multi();

	Promise<Void> unwatch();

	Promise<Void> watch(String key, String... otherKeys);
	// endregion
}
