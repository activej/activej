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

package io.activej.redis;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

enum Command {
	// Connection
	AUTH, CLIENT_GETNAME, CLIENT_SETNAME, CLIENT_PAUSE, ECHO, PING, QUIT, SELECT,


	// Server
	FLUSHALL,


	// Keys
	DEL, DUMP, EXISTS, EXPIRE, EXPIREAT, KEYS, MIGRATE, MOVE, OBJECT_ENCODING, OBJECT_FREQ, OBJECT_HELP,
	OBJECT_IDLETIME, OBJECT_REFCOUNT, PERSIST, PEXPIRE, PEXPIREAT, PTTL, RANDOMKEY, RENAME, RENAMENX,
	RESTORE, SCAN, SORT, TOUCH, TTL, TYPE, UNLINK, WAIT,


	// Strings
	APPEND, BITCOUNT, BITOP, BITPOS, DECR, DECRBY, GET, GETBIT, GETRANGE, GETSET, INCR, INCRBY, INCRBYFLOAT,
	MGET, MSET, MSETNX, PSETEX, SET, SETBIT, SETEX, SETNX, SETRANGE, STRLEN,


	// Lists
	BLPOP, BRPOP, BRPOPLPUSH, LINDEX, LINSERT, LLEN, LPOP, LPOS, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM,
	RPOP, RPOPLPUSH, RPUSH, RPUSHX,


	// Sets
	SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS, SMOVE, SPOP, SRANDMEMBER, SREM,
	SSCAN, SUNION, SUNIONSTORE,


	// Hashes
	HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSCAN, HSET, HSETNX, HSTRLEN, HVALS,


	// Sorted Sets
	BZPOPMIN, BZPOPMAX, ZADD, ZCARD, ZCOUNT, ZINCRBY, ZINTERSTORE, ZLEXCOUNT, ZPOPMAX, ZPOPMIN, ZRANGE, ZRANGEBYLEX,
	ZREVRANGEBYLEX, ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE,
	ZREVRANGEBYSCORE, ZREVRANK, ZSCAN, ZSCORE, ZUNIONSTORE,


	// HyperLogLog
	PFADD, PFCOUNT, PFMERGE,


	// Geo
	GEOADD, GEOHASH, GEOPOS, GEODIST, GEORADIUS, GEORADIUSBYMEMBER,

	// Transactions
	DISCARD, EXEC, MULTI, UNWATCH, WATCH;

	private final List<byte[]> parts;

	Command() {
		String[] stringParts = name().split("_");
		List<byte[]> parts = new ArrayList<>(stringParts.length);
		for (String stringPart : stringParts) {
			parts.add(stringPart.getBytes());
		}
		this.parts = unmodifiableList(parts);
	}

	public List<byte[]> getParts() {
		return parts;
	}
}
