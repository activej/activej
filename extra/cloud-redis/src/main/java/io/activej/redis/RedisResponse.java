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

import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class defines how a Redis response should be parsed.
 * There are some predefined common constant {@code RedisResponse}s for Redis data types.
 * A user can extend this class to provide a more specialized response parser and a more
 * complex in terms of a given Redis command/response.
 *
 * @param <T> result of parsing a Redis response
 */
public abstract class RedisResponse<T> {

	/**
	 * Parses a response data into result of some type
	 *
	 * @param data Redis response buffer
	 * @return result of parsing a Redis response
	 * @throws MalformedDataException if the response is malformed
	 *                                (either server is misbehaving or parser is incorrect)
	 * @throws NeedMoreDataException  when there is not enough bytes in {@code data} to parse
	 *                                a Redis response
	 */
	public abstract T parse(RESPv2 data) throws MalformedDataException, NeedMoreDataException;

	/**
	 * Allows mapping a {@code RedisResponse} of some type to a {@code RedisResponse}
	 * of some other type.
	 *
	 * @param fn  mapping function
	 * @param <V> resulting type
	 * @return a mapped {@code RedisResponse}
	 */
	public <V> RedisResponse<V> map(Mapping<T, V> fn) {
		return new RedisResponse<>() {
			@Override
			public V parse(RESPv2 data) throws MalformedDataException {
				T value = RedisResponse.this.parse(data);
				return value != null ? fn.map(value) : null;
			}
		};
	}

	/**
	 * Mapping interface that may throw {@link MalformedDataException}
	 *
	 * @param <T> the type of the input
	 * @param <R> the type of the output
	 */
	public interface Mapping<T, R> {
		R map(T value) throws MalformedDataException;
	}

	/**
	 * Skips a received Redis response
	 */
	public static final RedisResponse<Void> SKIP = new RedisResponse<>() {
		@Override
		public Void parse(RESPv2 data) throws MalformedDataException {
			data.skipObject();
			return null;
		}
	};

	/**
	 * Parses an arbitrary Redis response.
	 * <p>
	 * May return either of:
	 * <ul>
	 *     <li><b>{@code String}</b> - Redis Simple String</li>
	 *     <li><b>{@code Long}</b> - Redis Integer</li>
	 *     <li><b>{@code byte[]}</b> - Redis Bulk String</li>
	 *     <li><b>{@code ServerError}</b> - Redis Error</li>
	 *     <li><b>{@code null}</b> - Redis Nil</li>
	 *     <li><b>{@code Object[]}</b> - Redis Array that may contain any type listed here</li>
	 * </ul>
	 */
	public static final RedisResponse<Object> OBJECT = new RedisResponse<>() {
		@Override
		public @Nullable Object parse(RESPv2 data) throws MalformedDataException {
			return data.readObject();
		}
	};

	/**
	 * Parses a Redis Simple String
	 */
	public static final RedisResponse<String> STRING = new RedisResponse<>() {
		@Override
		public String parse(RESPv2 data) throws MalformedDataException {
			return data.readString();
		}
	};

	/**
	 * Parses a Redis Bulk String as an array of bytes (may parse {@code null} indicating Redis Nil)
	 */
	public static final RedisResponse<byte []> BYTES = new RedisResponse<>() {
		@Override
		public byte @Nullable [] parse(RESPv2 data) throws MalformedDataException {
			return data.readBytes();
		}
	};

	/**
	 * Parses a Redis Bulk String as a UTF-8 string (may parse {@code null} indicating Redis Nil)
	 */
	public static final RedisResponse<String> BYTES_UTF8 = new RedisResponse<>() {
		@Override
		public @Nullable String parse(RESPv2 data) throws MalformedDataException {
			return data.readBytes(UTF_8);
		}
	};

	/**
	 * Parses a Redis Bulk String as an ISO 8859-1 string (may parse {@code null} indicating Redis Nil)
	 */
	public static final RedisResponse<String> BYTES_ISO_8859_1 = new RedisResponse<>() {
		@Override
		public String parse(RESPv2 data) throws MalformedDataException {
			return data.readBytes(ISO_8859_1);
		}
	};

	/**
	 * Parses a Redis Integer as {@code Long}
	 */
	public static final RedisResponse<Long> LONG = new RedisResponse<>() {
		@Override
		public Long parse(RESPv2 data) throws MalformedDataException {
			return data.readLong();
		}
	};

	/**
	 * Parses a Redis Integer as {@code Boolean}. Redis Integer {@code 0} means {@code false},
	 * any other Redis Integer is interpreted as {@code true}
	 */
	public static final RedisResponse<Boolean> BOOLEAN = new RedisResponse<>() {
		@Override
		public Boolean parse(RESPv2 data) throws MalformedDataException {
			return data.readLong() != 0;
		}
	};

	/**
	 * Parses a Redis Array (may parse {@code null} indicating Redis Nil)
	 * <p>
	 * Array may contain any of:
	 * <ul>
	 *     <li><b>{@code String}</b> - Redis Simple String</li>
	 *     <li><b>{@code Long}</b> - Redis Integer</li>
	 *     <li><b>{@code byte[]}</b> - Redis Bulk String</li>
	 *     <li><b>{@code ServerError}</b> - Redis Error</li>
	 *     <li><b>{@code null}</b> - Redis Nil</li>
	 *     <li><b>{@code Object[]}</b> - Redis Array that may contain any type listed here</li>
	 * </ul>
	 */
	public static final RedisResponse<Object[]> ARRAY = new RedisResponse<>() {
		@Override
		public @Nullable Object @Nullable [] parse(RESPv2 data) throws MalformedDataException {
			return data.readObjectArray();
		}
	};

	/**
	 * Parses a Redis Simple String 'OK'
	 */
	public static final RedisResponse<Void> OK = new RedisResponse<>() {
		@Override
		public Void parse(RESPv2 data) throws MalformedDataException {
			data.readOk();
			return null;
		}
	};

	/**
	 * Parses a Redis Error
	 */
	public static final RedisResponse<ServerError> ERROR = new RedisResponse<>() {
		@Override
		public ServerError parse(RESPv2 data) throws MalformedDataException {
			return data.readError();
		}
	};

	static final RedisResponse<Void> QUEUED = new RedisResponse<>() {
		@Override
		public Void parse(RESPv2 data) throws MalformedDataException {
			data.readQueued();
			return null;
		}
	};

}
