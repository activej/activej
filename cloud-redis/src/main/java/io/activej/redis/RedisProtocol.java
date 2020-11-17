package io.activej.redis;

import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.ParseException;
import org.jetbrains.annotations.Nullable;

public interface RedisProtocol {
	int encode(byte[] array, int offset, RedisCommand item) throws ArrayIndexOutOfBoundsException;

	@Nullable
	RedisResponse tryDecode(ByteBufQueue queue) throws ParseException;
}
