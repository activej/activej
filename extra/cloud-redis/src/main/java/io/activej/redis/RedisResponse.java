package io.activej.redis;

import io.activej.common.exception.MalformedDataException;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class RedisResponse<T> {
	public abstract T parse(RESPv2 data) throws MalformedDataException;

	public <V> RedisResponse<V> map(Mapping<T, V> fn) {
		return new RedisResponse<V>() {
			@Override
			public V parse(RESPv2 data) throws MalformedDataException {
				T value = RedisResponse.this.parse(data);
				return value != null ? fn.map(value) : null;
			}
		};
	}

	public interface Mapping<T, R> {
		R map(T value) throws MalformedDataException;
	}

	public static final RedisResponse<Void> SKIP = new RedisResponse<Void>() {
		@Override
		public Void parse(RESPv2 data) throws MalformedDataException {
			data.skipObject();
			return null;
		}
	};

	public static final RedisResponse<Object> OBJECT = new RedisResponse<Object>() {
		@Override
		public Object parse(RESPv2 data) throws MalformedDataException {
			return data.readObject();
		}
	};

	public static final RedisResponse<String> STRING = new RedisResponse<String>() {
		@Override
		public String parse(RESPv2 data) throws MalformedDataException {
			return data.readString();
		}
	};

	public static final RedisResponse<byte[]> BYTES = new RedisResponse<byte[]>() {
		@Override
		public byte[] parse(RESPv2 data) throws MalformedDataException {
			return data.readBytes();
		}
	};

	public static final RedisResponse<String> BYTES_UTF8 = new RedisResponse<String>() {
		@Override
		public String parse(RESPv2 data) throws MalformedDataException {
			return data.readBytes(UTF_8);
		}
	};

	public static final RedisResponse<String> BYTES_ISO_8859_1 = new RedisResponse<String>() {
		@Override
		public String parse(RESPv2 data) throws MalformedDataException {
			return data.readBytes(ISO_8859_1);
		}
	};

	public static final RedisResponse<Long> LONG = new RedisResponse<Long>() {
		@Override
		public Long parse(RESPv2 data) throws MalformedDataException {
			return data.readLong();
		}
	};

	public static final RedisResponse<Boolean> BOOLEAN = new RedisResponse<Boolean>() {
		@Override
		public Boolean parse(RESPv2 data) throws MalformedDataException {
			return data.readLong() != 0;
		}
	};

	public static final RedisResponse<Object[]> ARRAY = new RedisResponse<Object[]>() {
		@Override
		public Object[] parse(RESPv2 data) throws MalformedDataException {
			return data.parseObjectArray();
		}
	};

	public static final RedisResponse<Void> OK = new RedisResponse<Void>() {
		@Override
		public Void parse(RESPv2 data) throws MalformedDataException {
			data.readOk();
			return null;
		}
	};

}
