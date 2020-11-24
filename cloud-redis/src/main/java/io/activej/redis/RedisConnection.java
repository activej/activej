package io.activej.redis;

import io.activej.common.api.ParserFunction;
import io.activej.common.exception.parse.ParseException;
import io.activej.net.connection.Connection;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.redis.api.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.redis.Utils.*;
import static io.activej.redis.api.BitOperator.NOT;
import static io.activej.redis.api.Command.*;
import static java.util.stream.Collectors.toList;

public final class RedisConnection implements RedisAPI, Connection {
	private static final RedisException IN_POOL = new RedisException(RedisConnection.class, "Connection is in pool");
	private static final RedisException ACTIVE_COMMANDS = new RedisException(RedisConnection.class, "Cannot return to pool, there are ongoing commands");
	private static final RedisException UNEXPECTED_NIL = new RedisException(RedisConnection.class, "Received unexpected 'NIL' response");
	private static final RedisException UNEXPECTED_SIZE_OF_ARRAY = new RedisException(RedisConnection.class, "Received array of unexpected size");
	private static final RedisException UNEXPECTED_TYPES_IN_ARRAY = new RedisException(RedisConnection.class, "Received array with elements of unexpected type");
	private static final RedisException UNEVEN_MAP = new RedisException(RedisConnection.class, "Map with uneven keys and values");

	private final RedisClient client;
	private final RedisMessaging messaging;
	private final Charset charset;

	private final Queue<SettablePromise<RedisResponse>> callbackDeque = new ArrayDeque<>();

	private boolean closed;
	boolean inPool;

	RedisConnection(RedisClient client, RedisMessaging messaging, Charset charset) {
		this.client = client;
		this.messaging = messaging;
		this.charset = charset;
	}

	// region Redis API
	// region connection
	@Override
	public Promise<String> auth(String password) {
		return send(RedisCommand.of(AUTH, charset, password), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> auth(String username, String password) {
		return send(RedisCommand.of(AUTH, charset, username, password), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> clientSetname(String connectionName) {
		return send(RedisCommand.of(CLIENT_SETNAME, charset, connectionName), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<@Nullable String> clientGetname() {
		return send(RedisCommand.of(CLIENT_GETNAME, charset), this::parseBulkString);
	}

	@Override
	public Promise<String> clientPause(Duration pauseDuration) {
		return send(RedisCommand.of(CLIENT_PAUSE, charset, String.valueOf(pauseDuration.toMillis())), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> echo(String message) {
		return send(RedisCommand.of(ECHO, charset, message), this::parseBulkString);
	}

	@Override
	public Promise<String> ping() {
		return send(RedisCommand.of(PING, charset), this::parseString);
	}

	@Override
	public Promise<String> ping(String message) {
		return send(RedisCommand.of(PING, charset, message), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Void> quit() {
		return send(RedisCommand.of(QUIT, charset), this::expectOk)
				.then(messaging::sendEndOfStream)
				.whenComplete(this::close);
	}

	@Override
	public Promise<String> select(int dbIndex) {
		checkArgument(dbIndex >= 0, "Negative DB index");
		return send(RedisCommand.of(SELECT, charset, String.valueOf(dbIndex)), RedisConnection::parseSimpleString);
	}
	// endregion

	// region keys
	@Override
	public Promise<Long> del(String key, String... otherKeys) {
		return send(RedisCommand.of(DEL, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<byte[]> dump(String key) {
		return send(RedisCommand.of(DUMP, charset, key), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> exists(String key, String... otherKeys) {
		return send(RedisCommand.of(EXISTS, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> expire(String key, long ttlSeconds) {
		return send(RedisCommand.of(EXPIRE, charset, key, String.valueOf(ttlSeconds)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> expireat(String key, long unixTimestampSeconds) {
		return send(RedisCommand.of(EXPIREAT, charset, key, String.valueOf(unixTimestampSeconds)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> keys(String pattern) {
		return send(RedisCommand.of(KEYS, charset, pattern), this::parseStrings);
	}

	@Override
	public Promise<Long> move(String key, int dbIndex) {
		checkArgument(dbIndex >= 0, "Negative DB index");
		return send(RedisCommand.of(MOVE, charset, key, String.valueOf(dbIndex)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> persist(String key) {
		return send(RedisCommand.of(PERSIST, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pexpire(String key, long ttlMillis) {
		return send(RedisCommand.of(PEXPIRE, charset, key, String.valueOf(ttlMillis)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pexpireat(String key, long unixTimestampMillis) {
		return send(RedisCommand.of(PEXPIREAT, charset, key, String.valueOf(unixTimestampMillis)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pttl(String key) {
		return send(RedisCommand.of(PTTL, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> randomkey() {
		return send(RedisCommand.of(RANDOMKEY, charset), this::parseBulkString);
	}

	@Override
	public Promise<String> rename(String key, String newKey) {
		return send(RedisCommand.of(RENAME, charset, key, newKey), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> renamenx(String key, String newKey) {
		return send(RedisCommand.of(RENAMENX, charset, key, newKey), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Void> restore(String key, Duration ttl, byte[] dump) {
		return send(RedisCommand.of(RESTORE, key.getBytes(charset), String.valueOf(ttl.toMillis()).getBytes(charset), dump), this::expectOk);
	}

	@Override
	public Promise<Long> touch(String key, String... otherKeys) {
		return send(RedisCommand.of(TOUCH, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> ttl(String key) {
		return send(RedisCommand.of(TTL, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<RedisType> type(String key) {
		return send(RedisCommand.of(TYPE, charset, key), RedisConnection::parseType);
	}

	@Override
	public Promise<Long> unlink(String key, String... otherKeys) {
		return send(RedisCommand.of(UNLINK, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> wait(int numberOfReplicas, long timeoutMillis) {
		return send(RedisCommand.of(WAIT, charset, String.valueOf(numberOfReplicas), String.valueOf(timeoutMillis)), RedisConnection::parseInteger);
	}
	// endregion

	// region strings
	@Override
	public Promise<Long> append(String key, String value) {
		return append(key, value.getBytes(charset));
	}

	@Override
	public Promise<Long> append(String key, byte[] value) {
		return send(RedisCommand.of(APPEND, key.getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitcount(String key) {
		return send(RedisCommand.of(BITCOUNT, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitcount(String key, int start, int end) {
		return send(RedisCommand.of(BITCOUNT, charset, key, String.valueOf(start), String.valueOf(end)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitop(BitOperator operator, String destKey, String sourceKey, String... otherSourceKeys) {
		checkArgument(operator != NOT || otherSourceKeys.length == 0, "BITOP NOT must be called with a single source key");
		return send(RedisCommand.of(BITOP, charset, list(operator.name(), destKey, sourceKey, otherSourceKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitpos(String key, boolean bitIsSet) {
		return send(RedisCommand.of(BITPOS, charset, key, bitIsSet ? "1" : "0"), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitpos(String key, boolean bitIsSet, int start) {
		return send(RedisCommand.of(BITPOS, charset, key, bitIsSet ? "1" : "0", String.valueOf(start)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitpos(String key, boolean bitIsSet, int start, int end) {
		return send(RedisCommand.of(BITPOS, charset, key, bitIsSet ? "1" : "0", String.valueOf(start), String.valueOf(end)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> decr(String key) {
		return send(RedisCommand.of(DECR, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> decrby(String key, long decrByValue) {
		return send(RedisCommand.of(DECRBY, charset, key, String.valueOf(decrByValue)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> get(String key) {
		return send(RedisCommand.of(GET, charset, key), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> getAsBinary(String key) {
		return send(RedisCommand.of(GET, charset, key), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> getbit(String key, int offset) {
		return send(RedisCommand.of(GETBIT, charset, key, String.valueOf(offset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> getrange(String key, int start, int end) {
		return send(RedisCommand.of(GETRANGE, charset, key, String.valueOf(start), String.valueOf(end)), this::parseBulkString);
	}

	@Override
	public Promise<byte[]> getrangeAsBinary(String key, int start, int end) {
		return send(RedisCommand.of(GETRANGE, charset, key, String.valueOf(start), String.valueOf(end)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable String> getset(String key, String value) {
		return send(RedisCommand.of(GETSET, charset, key, value), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> getset(String key, byte[] value) {
		return send(RedisCommand.of(GETSET, key.getBytes(charset), value), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> incr(String key) {
		return send(RedisCommand.of(INCR, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> incrby(String key, long incrByValue) {
		return send(RedisCommand.of(INCRBY, charset, key, String.valueOf(incrByValue)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> incrbyfloat(String key, double incrByFloatValue) {
		return send(RedisCommand.of(INCRBYFLOAT, charset, key, String.valueOf(incrByFloatValue)), this::parseDouble);
	}

	@Override
	public Promise<List<@Nullable String>> mget(String key, String... otherKeys) {
		return send(RedisCommand.of(MGET, charset, list(key, otherKeys)), response -> parseNullableArray(response, byte[].class, bytes -> new String(bytes, charset)))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<List<@Nullable byte[]>> mgetAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(MGET, charset, list(key, otherKeys)), response -> parseNullableArray(response, byte[].class, Function.identity()))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<Void> mset(Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = entries.entrySet().stream()
				.flatMap(entry -> Stream.of(entry.getKey().getBytes(charset), entry.getValue()))
				.collect(toList());
		return send(RedisCommand.of(MSET, args), this::expectOk);
	}

	@Override
	public Promise<Void> mset(String key, String value, String... otherKeysAndValues) {
		checkArgument(otherKeysAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(MSET, charset, list(key, value, otherKeysAndValues)), this::expectOk);
	}

	@Override
	public Promise<Long> msetnx(Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = entries.entrySet().stream()
				.flatMap(entry -> Stream.of(entry.getKey().getBytes(charset), entry.getValue()))
				.collect(toList());
		return send(RedisCommand.of(MSETNX, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> msetnx(String key, String value, String... otherKeysAndValues) {
		checkArgument(otherKeysAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(MSETNX, charset, list(key, value, otherKeysAndValues)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> psetex(String key, long millis, String value) {
		return send(RedisCommand.of(PSETEX, charset, key, String.valueOf(millis), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> psetex(String key, long millis, byte[] value) {
		return send(RedisCommand.of(PSETEX, key.getBytes(charset), String.valueOf(millis).getBytes(charset), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<@Nullable String> set(String key, String value, SetModifier... modifiers) {
		return set(key, value.getBytes(charset), modifiers);
	}

	@Override
	public Promise<@Nullable String> set(String key, byte[] value, SetModifier... modifiers) {
		RedisCommand command;
		if (modifiers.length == 0) {
			command = RedisCommand.of(SET, key.getBytes(charset), value);
		} else {
			checkSetModifiers(modifiers);

			List<byte[]> arguments = new ArrayList<>(modifiers.length + 2);
			arguments.add(key.getBytes(charset));
			arguments.add(value);
			for (SetModifier modifier : modifiers) {
				modifier.getArguments().stream()
						.map(arg -> arg.getBytes(charset))
						.forEach(arguments::add);
			}
			command = RedisCommand.of(SET, arguments);
		}
		return send(command, this::parseString);
	}

	@Override
	public Promise<Long> setbit(String key, int offset, int value) {
		checkArgument(value == 0 || value == 1, "Only values 1 and 0 are allowed");
		checkArgument(offset >= 0, "Offset must not be less than 0");
		return send(RedisCommand.of(SETBIT, charset, key, String.valueOf(offset), String.valueOf(value)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> setex(String key, long seconds, String value) {
		return send(RedisCommand.of(SETEX, charset, key, String.valueOf(seconds), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> setex(String key, long seconds, byte[] value) {
		return send(RedisCommand.of(SETEX, key.getBytes(charset), String.valueOf(seconds).getBytes(charset), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Long> setnx(String key, String value) {
		return send(RedisCommand.of(SETNX, charset, key, value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> setnx(String key, byte[] value) {
		return send(RedisCommand.of(SETNX, key.getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> setrange(String key, int offset, String value) {
		return send(RedisCommand.of(SETRANGE, charset, key, String.valueOf(offset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> setrange(String key, int offset, byte[] value) {
		return send(RedisCommand.of(SETRANGE, key.getBytes(charset), String.valueOf(offset).getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> strlen(String key) {
		return send(RedisCommand.of(STRLEN, charset, key), RedisConnection::parseInteger);
	}
	// endregion

	// region lists
	@Override
	public Promise<@Nullable ListPopResult> blpop(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BLPOP, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseListPopResult);
	}

	@Override
	public Promise<@Nullable ListPopResult> brpop(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BRPOP, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseListPopResult);
	}

	@Override
	public Promise<@Nullable String> brpoplpush(String source, String target, double timeoutSeconds) {
		String s = String.valueOf(timeoutSeconds);
		return send(RedisCommand.of(BRPOPLPUSH, charset, source, target, s), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> brpoplpushAsBinary(String source, String target, double timeoutSeconds) {
		return send(RedisCommand.of(BRPOPLPUSH, charset, source, target, String.valueOf(timeoutSeconds)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable String> lindex(String key, long index) {
		return send(RedisCommand.of(LINDEX, charset, key, String.valueOf(index)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> lindexAsBinary(String key, long index) {
		return send(RedisCommand.of(LINDEX, charset, key, String.valueOf(index)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> linsert(String key, InsertPosition position, String pivot, String element) {
		return send(RedisCommand.of(LINSERT, charset, key, position.name(), pivot, element), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> linsert(String key, InsertPosition position, byte[] pivot, byte[] element) {
		return send(RedisCommand.of(LINSERT, key.getBytes(charset), position.name().getBytes(charset), pivot, element), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> llen(String key) {
		return send(RedisCommand.of(LLEN, key.getBytes(charset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> lpop(String key) {
		return send(RedisCommand.of(LPOP, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> lpopAsBinary(String key) {
		return send(RedisCommand.of(LPOP, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable Long> lpos(String key, String element, LposModifier... modifiers) {
		return lpos(key, element.getBytes(charset), modifiers);
	}

	@Override
	public Promise<@Nullable Long> lpos(String key, byte[] element, LposModifier... modifiers) {
		RedisCommand command;
		if (modifiers.length == 0) {
			command = RedisCommand.of(LPOS, key.getBytes(charset), element);
		} else {
			checkLposModifiers(modifiers);

			List<byte[]> arguments = new ArrayList<>(modifiers.length + 2);
			arguments.add(key.getBytes(charset));
			arguments.add(element);
			for (LposModifier modifier : modifiers) {
				modifier.getArguments().stream()
						.map(arg -> arg.getBytes(charset))
						.forEach(arguments::add);
			}
			command = RedisCommand.of(LPOS, arguments);
		}
		return send(command, RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<List<Long>> lpos(String key, String element, int count, LposModifier... modifiers) {
		return lpos(key, element.getBytes(charset), count, modifiers);
	}

	@Override
	public Promise<List<Long>> lpos(String key, byte[] element, int count, LposModifier... modifiers) {
		checkArgument(count >= 0, "COUNT cannot be negative");
		checkLposModifiers(modifiers);

		List<byte[]> arguments = new ArrayList<>(modifiers.length + 4);
		arguments.add(key.getBytes(charset));
		arguments.add(element);
		arguments.add(LposModifier.COUNT.getBytes(charset));
		arguments.add(String.valueOf(count).getBytes(charset));
		for (LposModifier modifier : modifiers) {
			modifier.getArguments().stream()
					.map(arg -> arg.getBytes(charset))
					.forEach(arguments::add);
		}
		return send(RedisCommand.of(LPOS, arguments), response -> parseArray(response, Long.class, Function.identity()));
	}

	@Override
	public Promise<Long> lpush(String key, String element, String... otherElements) {
		return send(RedisCommand.of(LPUSH, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lpush(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(LPUSH, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lpushx(String key, String element, String... otherElements) {
		return send(RedisCommand.of(LPUSHX, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lpushx(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(LPUSHX, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> lrange(String key, long start, long stop) {
		return send(RedisCommand.of(LRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), response ->
				parseArray(response, byte[].class, bytes -> new String(bytes, charset), false));
	}

	@Override
	public Promise<List<byte[]>> lrangeAsBinary(String key, long start, long stop) {
		return send(RedisCommand.of(LRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), response ->
				parseArray(response, byte[].class, Function.identity(), false));
	}

	@Override
	public Promise<Long> lrem(String key, long count, String element) {
		return send(RedisCommand.of(LREM, charset, key, String.valueOf(count), String.valueOf(element)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lrem(String key, long count, byte[] element) {
		return send(RedisCommand.of(LREM, key.getBytes(charset), String.valueOf(count).getBytes(charset), element), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> lset(String key, long index, String element) {
		return send(RedisCommand.of(LSET, charset, key, String.valueOf(index), element), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> lset(String key, long index, byte[] element) {
		return send(RedisCommand.of(LSET, key.getBytes(charset), String.valueOf(index).getBytes(charset), element), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> ltrim(String key, long start, long stop) {
		return send(RedisCommand.of(LTRIM, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<@Nullable String> rpop(String key) {
		return send(RedisCommand.of(RPOP, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> rpopAsBinary(String key) {
		return send(RedisCommand.of(RPOP, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable String> rpoplpush(String source, String destination) {
		return send(RedisCommand.of(RPOPLPUSH, charset, source, destination), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> rpoplpushAsBinary(String source, String destination) {
		return send(RedisCommand.of(RPOPLPUSH, charset, source, destination), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> rpush(String key, String element, String... otherElements) {
		return send(RedisCommand.of(RPUSH, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> rpush(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(RPUSH, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> rpushx(String key, String element, String... otherElements) {
		return send(RedisCommand.of(RPUSHX, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> rpushx(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(RPUSHX, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}
	// endregion

	// region sets
	@Override
	public Promise<Long> sadd(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(SADD, charset, list(key, member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> sadd(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(SADD, list(key.getBytes(charset), member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> scard(String key) {
		return send(RedisCommand.of(SCARD, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> sdiff(String key, String... otherKeys) {
		return send(RedisCommand.of(SDIFF, charset, list(key, otherKeys)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> sdiffAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(SDIFF, charset, list(key, otherKeys)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> sdiffstore(String destination, String key, String... otherKeys) {
		return send(RedisCommand.of(SDIFFSTORE, charset, list(destination, key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> sinter(String key, String... otherKeys) {
		return send(RedisCommand.of(SINTER, charset, list(key, otherKeys)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> sinterAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(SINTER, charset, list(key, otherKeys)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> sinterstore(String destination, String key, String... otherKeys) {
		return send(RedisCommand.of(SINTERSTORE, charset, list(destination, key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> sismember(String key, String member) {
		return sismember(key, member.getBytes(charset));
	}

	@Override
	public Promise<Long> sismember(String key, byte[] member) {
		return send(RedisCommand.of(SISMEMBER, key.getBytes(charset), member), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> smembers(String key) {
		return send(RedisCommand.of(SMEMBERS, key.getBytes(charset)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> smembersAsBinary(String key) {
		return send(RedisCommand.of(SMEMBERS, key.getBytes(charset)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> smove(String source, String destination, String member) {
		return smove(source, destination, member.getBytes(charset));
	}

	@Override
	public Promise<Long> smove(String source, String destination, byte[] member) {
		return send(RedisCommand.of(SMOVE, source.getBytes(charset), destination.getBytes(charset), member), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> spop(String key) {
		return send(RedisCommand.of(SPOP, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> spopAsBinary(String key) {
		return send(RedisCommand.of(SPOP, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<List<String>> spop(String key, long count) {
		return send(RedisCommand.of(SPOP, charset, key, String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> spopAsBinary(String key, long count) {
		return send(RedisCommand.of(SPOP, charset, key, String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<@Nullable String> srandmember(String key) {
		return send(RedisCommand.of(SRANDMEMBER, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> srandmemberAsBinary(String key) {
		return send(RedisCommand.of(SRANDMEMBER, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<List<String>> srandmember(String key, long count) {
		return send(RedisCommand.of(SRANDMEMBER, charset, key, String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> srandmemberAsBinary(String key, long count) {
		return send(RedisCommand.of(SRANDMEMBER, charset, key, String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> srem(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(SREM, charset, list(key, member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> srem(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(SREM, list(key.getBytes(charset), member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> sunion(String key, String... otherKeys) {
		return send(RedisCommand.of(SUNION, charset, list(key, otherKeys)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> sunionAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(SUNION, charset, list(key, otherKeys)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> sunionstore(String destination, String key, String... otherKeys) {
		return send(RedisCommand.of(SUNIONSTORE, charset, list(destination, key, otherKeys)), RedisConnection::parseInteger);
	}
	// endregion

	// region hashes
	@Override
	public Promise<Long> hdel(String key, String field, String... otherFields) {
		return send(RedisCommand.of(HDEL, charset, list(key, field, otherFields)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hexists(String key, String field) {
		return send(RedisCommand.of(HEXISTS, charset, key, field), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> hget(String key, String field) {
		return send(RedisCommand.of(HGET, charset, key, field), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> hgetAsBinary(String key, String field) {
		return send(RedisCommand.of(HGET, charset, key, field), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Map<String, String>> hgetall(String key) {
		return send(RedisCommand.of(HGETALL, key.getBytes(charset)), response -> parseMap(response, field -> new String(field, charset)));
	}

	@Override
	public Promise<Map<String, byte[]>> hgetallAsBinary(String key) {
		return send(RedisCommand.of(HGETALL, key.getBytes(charset)), response -> parseMap(response, v -> v));
	}

	@Override
	public Promise<Long> hincrby(String key, String field, long incrByValue) {
		return send(RedisCommand.of(HINCRBY, charset, key, field, String.valueOf(incrByValue)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> hincrbyfloat(String key, String field, double incrByValue) {
		return send(RedisCommand.of(HINCRBYFLOAT, charset, key, field, String.valueOf(incrByValue)), this::parseDouble);
	}

	@Override
	public Promise<List<String>> hkeys(String key) {
		return send(RedisCommand.of(HKEYS, key.getBytes(charset)), this::parseStrings);
	}

	@Override
	public Promise<Long> hlen(String key) {
		return send(RedisCommand.of(HLEN, key.getBytes(charset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<@Nullable String>> hmget(String key, String field, String... otherFields) {
		return send(RedisCommand.of(HMGET, charset, list(key, field, otherFields)), response -> parseNullableArray(response, byte[].class, bytes -> new String(bytes, charset)))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<List<@Nullable byte[]>> hmgetAsBinary(String key, String field, String... otherFields) {
		return send(RedisCommand.of(HMGET, charset, list(key, field, otherFields)), response -> parseNullableArray(response, byte[].class, Function.identity()))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<String> hmset(String key, Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = new ArrayList<>(entries.size() * 2 + 1);
		args.add(key.getBytes(charset));
		for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
			args.add(entry.getKey().getBytes(charset));
			args.add(entry.getValue());
		}
		return send(RedisCommand.of(HMSET, args), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> hmset(String key, String field, String value, String... otherFieldsAndValues) {
		checkArgument(otherFieldsAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(HMSET, charset, list(key, field, value, otherFieldsAndValues)), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Long> hset(String key, Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = new ArrayList<>(entries.size() * 2 + 1);
		args.add(key.getBytes(charset));
		for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
			args.add(entry.getKey().getBytes(charset));
			args.add(entry.getValue());
		}
		return send(RedisCommand.of(HSET, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hset(String key, String field, String value, String... otherFieldsAndValues) {
		checkArgument(otherFieldsAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(HSET, charset, list(key, field, value, otherFieldsAndValues)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hsetnx(String key, String field, String value) {
		return send(RedisCommand.of(HSETNX, charset, key, field, value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hsetnx(String key, String field, byte[] value) {
		return send(RedisCommand.of(HSETNX, key.getBytes(charset), field.getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hstrlen(String key, String field) {
		return send(RedisCommand.of(HSTRLEN, charset, key, field), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> hvals(String key) {
		return send(RedisCommand.of(HVALS, key.getBytes(charset)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> hvalsAsBinary(String key) {
		return send(RedisCommand.of(HVALS, key.getBytes(charset)), RedisConnection::parseBytes);
	}
	// endregion

	// region sorted sets
	@Override
	public Promise<@Nullable SetBlockingPopResult> bzpopmin(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BZPOPMIN, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseSetBlockingPopResult);
	}

	@Override
	public Promise<@Nullable SetBlockingPopResult> bzpopmax(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BZPOPMAX, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseSetBlockingPopResult);
	}

	@Override
	public Promise<Long> zadd(String key, Map<Double, String> entries, ZaddModifier... modifiers) {
		checkArgument(!entries.isEmpty(), "No entry to add");
		checkZaddModifiers(modifiers);

		List<byte[]> args = new ArrayList<>(entries.size() * 2 + modifiers.length + 1);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		for (Map.Entry<Double, String> entry : entries.entrySet()) {
			args.add(String.valueOf(entry.getKey()).getBytes(charset));
			args.add(entry.getValue().getBytes(charset));
		}
		return send(RedisCommand.of(ZADD, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> zaddIncr(String key, double score, String value, ZaddModifier... modifiers) {
		checkZaddModifiers(modifiers);
		List<byte[]> args = new ArrayList<>(4 + modifiers.length);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		args.add(ZaddModifier.INCR.getBytes(charset));

		args.add(String.valueOf(score).getBytes(charset));
		args.add(value.getBytes(charset));
		return send(RedisCommand.of(ZADD, args), this::parseDouble);
	}

	@Override
	public Promise<Long> zaddBinary(String key, Map<Double, byte[]> entries, ZaddModifier... modifiers) {
		checkArgument(!entries.isEmpty(), "No entry to add");
		checkZaddModifiers(modifiers);

		List<byte[]> args = new ArrayList<>(entries.size() * 2 + modifiers.length + 1);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		for (Map.Entry<Double, byte[]> entry : entries.entrySet()) {
			args.add(String.valueOf(entry.getKey()).getBytes(charset));
			args.add(entry.getValue());
		}
		return send(RedisCommand.of(ZADD, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> zaddIncr(String key, double score, byte[] value, ZaddModifier... modifiers) {
		checkZaddModifiers(modifiers);
		List<byte[]> args = new ArrayList<>(4 + modifiers.length);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		args.add(ZaddModifier.INCR.getBytes(charset));

		args.add(String.valueOf(score).getBytes(charset));
		args.add(value);
		return send(RedisCommand.of(ZADD, args), this::parseDouble);
	}

	@Override
	public Promise<Long> zcard(String key) {
		return send(RedisCommand.of(ZCARD, key.getBytes(charset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zcount(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZCOUNT, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> zincrby(String key, double increment, String member) {
		return send(RedisCommand.of(ZINCRBY, charset, key, String.valueOf(increment), member), this::parseDouble);
	}

	@Override
	public Promise<Double> zincrby(String key, double increment, byte[] member) {
		return send(RedisCommand.of(ZINCRBY, key.getBytes(charset), String.valueOf(increment).getBytes(charset), member), this::parseDouble);
	}

	@Override
	public Promise<Long> zinterstore(String destination, Aggregate aggregate, Map<String, Double> entries) {
		return doZstore(ZINTERSTORE, destination, aggregate, entries);
	}

	@Override
	public Promise<Long> zinterstore(String destination, Map<String, Double> entries) {
		return doZstore(ZINTERSTORE, destination, null, entries);
	}

	@Override
	public Promise<Long> zinterstore(String destination, @Nullable Aggregate aggregate, String key, String... otherKeys) {
		return doZstore(ZINTERSTORE, destination, aggregate, key, otherKeys);
	}

	@Override
	public Promise<Long> zinterstore(String destination, String key, String... otherKeys) {
		return doZstore(ZINTERSTORE, destination, null, key, otherKeys);
	}

	@Override
	public Promise<Long> zlexcount(String key, LexInterval interval) {
		return send(RedisCommand.of(ZLEXCOUNT, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable List<SetPopResult>> zpopmax(String key, long count) {
		if (count == 1) return send(RedisCommand.of(ZPOPMAX, key.getBytes(charset)), this::parseSetPopResults);
		return send(RedisCommand.of(ZPOPMAX, charset, key, String.valueOf(count)), this::parseSetPopResults);
	}

	@Override
	public Promise<@Nullable List<SetPopResult>> zpopmin(String key, long count) {
		if (count == 1) return send(RedisCommand.of(ZPOPMIN, key.getBytes(charset)), this::parseSetPopResults);
		return send(RedisCommand.of(ZPOPMIN, charset, key, String.valueOf(count)), this::parseSetPopResults);
	}

	@Override
	public Promise<List<String>> zrange(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangeAsBinary(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrangeWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrangeAsBinaryWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, v -> v, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<List<String>> zrangebylex(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangebylexAsBinary(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrangebylex(String key, LexInterval interval) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangebylexAsBinary(String key, LexInterval interval) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrevrangebylex(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebylexAsBinary(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrevrangebylex(String key, LexInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebylexAsBinary(String key, LexInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrangebyscore(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<String>> zrangebyscore(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangebyscoreAsBinary(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<byte[]>> zrangebyscoreAsBinary(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrangebyscoreWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<String, Double>> zrangebyscoreWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<@Nullable Long> zrank(String key, String member) {
		return send(RedisCommand.of(ZRANK, charset, key, member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<@Nullable Long> zrank(String key, byte[] member) {
		return send(RedisCommand.of(ZRANK, key.getBytes(charset), member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<Long> zrem(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(ZREM, charset, list(key, member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zrem(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(ZREM, list(key.getBytes(charset), member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zremrangebylex(String key, LexInterval interval) {
		return send(RedisCommand.of(ZREMRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zremrangebyrank(String key, long start, long stop) {
		return send(RedisCommand.of(ZREMRANGEBYRANK, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zremrangebyscore(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREMRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> zrevrange(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangeAsBinary(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrevrangeWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrevrangeAsBinaryWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, v -> v, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<List<String>> zrevrangebyscore(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebyscoreAsBinary(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebyscoreAsBinary(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrevrangebyscoreWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<String, Double>> zrevrangebyscoreWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrevrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrevrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<@Nullable Long> zrevrank(String key, String member) {
		return send(RedisCommand.of(ZREVRANK, charset, key, member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<@Nullable Long> zrevrank(String key, byte[] member) {
		return send(RedisCommand.of(ZREVRANK, key.getBytes(charset), member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<@Nullable Double> zscore(String key, String member) {
		return send(RedisCommand.of(ZSCORE, charset, key, member), this::parseNullableDouble);
	}

	@Override
	public Promise<@Nullable Double> zscore(String key, byte[] member) {
		return send(RedisCommand.of(ZSCORE, key.getBytes(charset), member), this::parseNullableDouble);
	}

	@Override
	public Promise<Long> zunionstore(String destination, Aggregate aggregate, Map<String, Double> entries) {
		return doZstore(ZUNIONSTORE, destination, aggregate, entries);
	}

	@Override
	public Promise<Long> zunionstore(String destination, Map<String, Double> entries) {
		return doZstore(ZUNIONSTORE, destination, null, entries);
	}

	@Override
	public Promise<Long> zunionstore(String destination, Aggregate aggregate, String key, String... otherKeys) {
		return doZstore(ZUNIONSTORE, destination, aggregate, key, otherKeys);
	}

	@Override
	public Promise<Long> zunionstore(String destination, String key, String... otherKeys) {
		return doZstore(ZUNIONSTORE, destination, null, key, otherKeys);
	}
	// endregion

	// region hyperloglog
	@Override
	public Promise<Long> pfadd(String key, String element, String... otherElements) {
		return send(RedisCommand.of(PFADD, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pfadd(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(PFADD, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pfcount(String key, String... otherKeys) {
		return send(RedisCommand.of(PFCOUNT, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Void> pfmerge(String destKey, String sourceKey, String... otherSourceKeys) {
		return send(RedisCommand.of(PFMERGE, charset, list(destKey, sourceKey, otherSourceKeys)), this::expectOk);
	}
	// endregion
	// endregion

	// region connection and pooling
	@Override
	public boolean isClosed() {
		if (closed) return true;
		if (!messaging.isClosed()) return false;
		close();
		return true;
	}

	@Override
	public Promise<Void> returnToPool() {
		if (!callbackDeque.isEmpty()) return Promise.ofException(ACTIVE_COMMANDS);
		if (closed) return Promise.ofException(CLOSE_EXCEPTION);
		if (inPool) return Promise.ofException(IN_POOL);
		client.returnConnection(this);
		return Promise.complete();
	}

	@Override
	public Promise<Void> close(@NotNull Throwable e) {
		if (closed) return Promise.complete();
		closed = true;
		return Promises.all(callbackDeque.iterator())
				.whenComplete(() -> {
					messaging.closeEx(e);
					client.onConnectionClose(this);
				});
	}
	// endregion

	private <T> Promise<T> send(RedisCommand command, Function<RedisResponse, Promise<@Nullable T>> responseParser) {
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		if (inPool) return Promise.ofException(IN_POOL);

		Promise<Void> sendPromise = messaging.send(command);
		Promise<RedisResponse> receivePromise = receive();

		return sendPromise.then(() -> receivePromise)
				.then(responseParser);
	}

	private Promise<RedisResponse> receive() {
		if (callbackDeque.isEmpty()) {
			Promise<RedisResponse> receivePromise = messaging.receive();
			if (receivePromise.isComplete()) return receivePromise;

			SettablePromise<RedisResponse> newCb = new SettablePromise<>();
			receivePromise
					.whenComplete(callbackDeque::remove)
					.whenComplete(newCb)
					.whenResult(this::onReceive);
			callbackDeque.offer(newCb);
			return newCb;
		} else {
			SettablePromise<RedisResponse> newCb = new SettablePromise<>();
			callbackDeque.offer(newCb);
			return newCb;
		}
	}

	private void onReceive() {
		while (!callbackDeque.isEmpty()) {
			Promise<RedisResponse> rcvPromise = messaging.receive()
					.whenComplete((result, e) -> callbackDeque.remove().trySet(result, e));
			if (!rcvPromise.isComplete()) {
				rcvPromise.whenResult(this::onReceive);
				break;
			}
		}
	}

	private static Promise<String> parseSimpleString(RedisResponse response) {
		if (response.isString()) {
			return Promise.of(response.getString());
		}
		return parseError(response);
	}

	private static Promise<Long> parseInteger(RedisResponse response) {
		if (response.isInteger()) {
			return Promise.of(response.getInteger());
		}
		return parseError(response);
	}

	private static Promise<@Nullable Long> parseNullableInteger(RedisResponse response) {
		if (response.isNil()) {
			return Promise.of(null);
		}
		if (response.isInteger()) {
			return Promise.of(response.getInteger());
		}
		return parseError(response);
	}

	private static Promise<RedisType> parseType(RedisResponse response) {
		return parseSimpleString(response)
				.then(type -> {
					try {
						return Promise.of(RedisType.valueOf(type.toUpperCase()));
					} catch (IllegalArgumentException e) {
						return Promise.ofException(new RedisException(RedisConnection.class, "Type '" + type + "' is not known"));
					}
				});
	}

	private static Promise<@Nullable byte[]> parseBulk(RedisResponse response) {
		if (response.isBytes()) {
			return Promise.of(response.getBytes());
		}
		if (response.isNil()) {
			return Promise.of(null);
		}
		return parseError(response);
	}

	private Promise<@Nullable String> parseBulkString(RedisResponse response) {
		return parseBulk(response)
				.map(bytes -> bytes == null ? null : new String(bytes, charset));
	}

	private Promise<@Nullable String> parseString(RedisResponse response) {
		return response.isString() ?
				parseSimpleString(response) :
				parseBulk(response)
						.map(bytes -> bytes == null ? null : new String(bytes, charset));
	}

	private static Promise<@Nullable List<?>> parseArray(RedisResponse response) {
		if (response.isArray()) {
			return Promise.of(response.getArray());
		}
		if (response.isNil()) {
			return Promise.of(null);
		}
		return parseError(response);
	}

	private static <T, U> Promise<@Nullable List<T>> parseArray(RedisResponse response, Class<U> expectedClass, Function<U, T> fn) {
		return parseArray(response, expectedClass, fn, false);
	}

	private static <T, U> Promise<@Nullable List<T>> parseNullableArray(RedisResponse response, Class<U> expectedClass, Function<U, T> fn) {
		return parseArray(response, expectedClass, fn, true);
	}

	@SuppressWarnings("unchecked")
	private static <T, U> Promise<@Nullable List<T>> parseArray(RedisResponse response, Class<U> expectedClass, Function<U, T> fn, boolean nullable) {
		return parseArray(response)
				.then(array -> {
					if (array == null || array.isEmpty()) return Promise.of((List<T>) array);

					List<T> result = new ArrayList<>(array.size());
					try {
						for (Object element : array) {
							if (element == null && nullable) {
								result.add(null);
								continue;
							}
							if (element == null) return unexpectedTypeException(expectedClass);

							result.add(fn.apply(expectedClass.cast(element)));
						}
					} catch (ClassCastException e) {
						return unexpectedTypeException(expectedClass);
					}

					return Promise.of(result);
				});
	}

	private static <T, U> Promise<@Nullable T> unexpectedTypeException(Class<U> expectedClass) {
		return Promise.ofException(new RedisException(RedisConnection.class,
				"Expected all array items to be of type '" + expectedClass.getSimpleName() +
						"', but some elements were not"));
	}

	private <T> Promise<Map<String, T>> parseMap(RedisResponse response, ParserFunction<byte[], T> valueFn) {
		return parseMap(response, keyBytes -> new String(keyBytes, charset), valueFn);
	}

	private static <K, V> Promise<Map<K, V>> parseMap(RedisResponse response, ParserFunction<byte[], K> keyFn, ParserFunction<byte[], V> valueFn) {
		if (response.isArray()) {
			List<?> array = response.getArray();
			if (array.size() % 2 != 0) {
				return Promise.ofException(UNEVEN_MAP);
			}
			Map<K, V> result = new LinkedHashMap<>(array.size());
			try {
				for (int i = 0; i < array.size(); ) {
					byte[] key = (byte[]) array.get(i++);
					byte[] value = (byte[]) array.get(i++);
					K keyK = keyFn.parse(key);
					V valueV = valueFn.parse(value);
					if (result.put(keyK, valueV) != null) {
						return Promise.ofException(new RedisException(RedisConnection.class, "Duplicate field: " + keyK));
					}
				}
				return Promise.of(result);
			} catch (ClassCastException e) {
				return RedisConnection.unexpectedTypeException(byte[].class);
			} catch (ParseException e) {
				return Promise.ofException(new RedisException(RedisConnection.class, "Could not parse value", e));
			}
		}
		return parseError(response);
	}

	private Promise<@Nullable ListPopResult> parseListPopResult(RedisResponse response) {
		return parseArray(response)
				.then(array -> {
					if (array == null) return Promise.of(null);
					if (array.size() != 2) return Promise.ofException(UNEXPECTED_SIZE_OF_ARRAY);
					byte[] foundKey;
					byte[] value;
					try {
						foundKey = (byte[]) array.get(0);
						value = (byte[]) array.get(1);
					} catch (ClassCastException e) {
						return Promise.ofException(UNEXPECTED_TYPES_IN_ARRAY);
					}

					return Promise.of(new ListPopResult(charset, new String(foundKey, charset), value));
				});
	}

	private Promise<@Nullable SetBlockingPopResult> parseSetBlockingPopResult(RedisResponse response) {
		return parseArray(response)
				.then(array -> {
					if (array == null) return Promise.of(null);
					if (array.size() != 3) return Promise.ofException(UNEXPECTED_SIZE_OF_ARRAY);
					byte[] foundKey;
					byte[] value;
					double score;
					try {
						foundKey = (byte[]) array.get(0);
						value = (byte[]) array.get(1);
						score = Double.parseDouble(new String((byte[]) array.get(2), charset));
					} catch (ClassCastException e) {
						return Promise.ofException(UNEXPECTED_TYPES_IN_ARRAY);
					}

					return Promise.of(new SetBlockingPopResult(charset, new String(foundKey, charset), value, score));
				});
	}

	private Promise<@Nullable List<SetPopResult>> parseSetPopResults(RedisResponse response) {
		return parseArray(response)
				.then(array -> {
					if (array == null) return Promise.of(null);
					if (array.size() % 2 != 0) return Promise.ofException(UNEXPECTED_SIZE_OF_ARRAY);

					List<SetPopResult> result = new ArrayList<>();
					for (int i = 0; i < array.size(); ) {
						byte[] value;
						double score;
						try {
							value = (byte[]) array.get(i++);
							score = Double.parseDouble(new String((byte[]) array.get(i++), charset));
						} catch (ClassCastException e) {
							return Promise.ofException(UNEXPECTED_TYPES_IN_ARRAY);
						}

						result.add(new SetPopResult(charset, value, score));
					}
					return Promise.of(result);
				});
	}

	private static <T> Promise<T> parseError(RedisResponse response) {
		if (response.isError()) {
			return Promise.ofException(response.getError());
		}
		return Promise.ofException(new RedisException(RedisConnection.class, "Unexpected response: " + response));
	}

	private Promise<List<String>> parseStrings(RedisResponse response) {
		return parseArray(response, byte[].class, bytes -> new String(bytes, charset))
				.then(RedisConnection::nonNull);
	}

	private static Promise<List<byte[]>> parseBytes(RedisResponse response) {
		return parseArray(response, byte[].class, Function.identity())
				.then(RedisConnection::nonNull);
	}

	private Promise<Double> parseDouble(RedisResponse response) {
		return doParseDouble(response, false);
	}

	private Promise<Double> parseNullableDouble(RedisResponse response) {
		return doParseDouble(response, true);
	}

	private Promise<Double> doParseDouble(RedisResponse response, boolean nullable) {
		return parseBulkString(response)
				.then(string -> {
					if (string == null){
						if (nullable) return Promise.of(null);
						return Promise.ofException(UNEXPECTED_NIL);
					}
					try {
						return Promise.of(Double.parseDouble(string));
					} catch (NumberFormatException e) {
						return Promise.ofException(new RedisException(RedisConnection.class, "Could not parse result as double: " + string));
					}
				});
	}

	private Promise<Void> expectOk(RedisResponse response) {
		return parseString(response)
				.then(result -> "OK".equals(result) ? Promise.complete() :
						Promise.ofException(new RedisException(RedisConnection.class, "Expected result to be 'OK', was: " + result)));
	}

	private static <T> Promise<@NotNull T> nonNull(@Nullable T value) {
		return value == null ?
				Promise.ofException(UNEXPECTED_NIL) :
				Promise.of(value);
	}

	private Promise<Long> doZstore(Command storeCommand, String destination, @Nullable Aggregate aggregate, Map<String, Double> entries) {
		checkArgument(!entries.isEmpty(), "No key specified");

		List<byte[]> args = new ArrayList<>(entries.size() + 5);
		args.add(destination.getBytes(charset));
		args.add(String.valueOf(entries.size()).getBytes(charset));
		List<byte[]> weights = new ArrayList<>();
		for (Map.Entry<String, Double> entry : entries.entrySet()) {
			args.add(entry.getKey().getBytes(charset));
			weights.add(String.valueOf(entry.getValue()).getBytes(charset));
		}
		args.add(WEIGHTS.getBytes(charset));
		args.addAll(weights);
		if (aggregate != null) {
			args.add(AGGREGATE.getBytes(charset));
			args.add(aggregate.name().getBytes(charset));
		}
		return send(RedisCommand.of(storeCommand, args), RedisConnection::parseInteger);
	}

	private Promise<Long> doZstore(Command storeCommand, String destination, @Nullable Aggregate aggregate, String key, String[] otherKeys) {
		List<byte[]> args = new ArrayList<>(otherKeys.length + 4);
		args.add(destination.getBytes(charset));
		args.add(String.valueOf(otherKeys.length + 1).getBytes(charset));
		args.add(key.getBytes(charset));
		for (String otherKey : otherKeys) {
			args.add(otherKey.getBytes(charset));
		}
		if (aggregate != null) {
			args.add(AGGREGATE.getBytes(charset));
			args.add(aggregate.name().getBytes(charset));
		}
		return send(RedisCommand.of(storeCommand, args), RedisConnection::parseInteger);
	}
}
