package io.activej.redis;

import io.activej.redis.api.ServerError;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static io.activej.common.Checks.checkNotNull;

public final class RedisResponse {
	@Nullable
	private final Long integer;

	@Nullable
	private final String string;

	@Nullable
	private final byte[] bytes;

	@Nullable
	private final List<?> array;

	@Nullable
	private final ServerError error;

	private final boolean isNil;

	private RedisResponse(@Nullable Long integer, @Nullable String string, @Nullable byte[] bytes, @Nullable List<?> array, @Nullable ServerError error, boolean isNil) {
		this.integer = integer;
		this.string = string;
		this.bytes = bytes;
		this.array = array;
		this.error = error;
		this.isNil = isNil;
	}

	public static RedisResponse integer(long value) {
		return new RedisResponse(value, null, null, null, null, false);
	}

	public static RedisResponse string(String value) {
		return new RedisResponse(null, value, null, null, null, false);
	}

	public static RedisResponse bytes(byte[] value) {
		return new RedisResponse(null, null, value, null, null, false);
	}

	public static RedisResponse array(List<?> value) {
		return new RedisResponse(null, null, null, value, null, false);
	}

	public static RedisResponse error(ServerError error) {
		return new RedisResponse(null, null, null, null, error, false);
	}

	public static RedisResponse nil() {
		return new RedisResponse(null, null, null, null, null, true);
	}

	public Long getInteger() {
		return checkNotNull(integer);
	}

	public String getString() {
		return checkNotNull(string);
	}

	public byte[] getBytes() {
		return checkNotNull(bytes);
	}

	public List<?> getArray() {
		return checkNotNull(array);
	}

	public ServerError getError() {
		return checkNotNull(error);
	}

	public boolean isNil() {
		return isNil;
	}

	public boolean isInteger() {
		return integer != null;
	}

	public boolean isString() {
		return string != null;
	}

	public boolean isBytes() {
		return bytes != null;
	}

	public boolean isError() {
		return error != null;
	}

	public boolean isArray() {
		return array != null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RedisResponse that = (RedisResponse) o;

		if (isNil != that.isNil) return false;
		if (!Objects.equals(integer, that.integer)) return false;
		if (!Objects.equals(string, that.string)) return false;
		if (!Arrays.equals(bytes, that.bytes)) return false;
		if (!Utils.deepEquals(array, that.array)) return false;
		return error != null && that.error != null ?
				error.getMessage().equals(that.error.getMessage()) :
				error == that.error;
	}

	@Override
	public int hashCode() {
		int result = integer != null ? integer.hashCode() : 0;
		result = 31 * result + (string != null ? string.hashCode() : 0);
		result = 31 * result + (bytes != null ? Arrays.hashCode(bytes) : 0);
		result = 31 * result + (array != null ? Utils.deepHashCode(array) : 0);
		result = 31 * result + (error != null ? error.getMessage().hashCode() : 0);
		result = 31 * result + (isNil ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		String result = "RedisResponse{";

		if (integer != null) result += "integer=" + integer;
		else if (string != null) result += "string='" + string + '\'';
		else if (bytes != null) result += "bytes=" + Arrays.toString(bytes);
		else if (array != null) result += "array=" + array;
		else if (error != null) result += "error=" + error;
		else result += "nil";

		result += '}';

		return result;
	}
}
