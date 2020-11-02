package io.activej.redis;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;
import io.activej.csp.binary.ByteBufsCodec;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static io.activej.csp.binary.ByteBufsDecoder.ofCrlfTerminatedBytes;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class RESPv2Codec implements ByteBufsCodec<RedisResponse, RedisCommand> {

	private static final byte[] CR_LF = {13, 10};
	private static final int CR_LF_LENGTH = CR_LF.length;
	private static final int INTEGER_MAX_LEN = String.valueOf(Long.MIN_VALUE).length();
	private static final int ARRAY_ESTIMATED_PADDING = 1 + CR_LF_LENGTH + INTEGER_MAX_LEN;
	private static final int ARG_ESTIMATED_PADDING = 1 + 2 * CR_LF_LENGTH + INTEGER_MAX_LEN;
	private static final int STRING_MAX_LEN = 512 * 1024 * 1024; // 512 MB

	private static final byte STRING_MARKER = '+';
	private static final byte ERROR_MARKER = '-';
	private static final byte INTEGER_MARKER = ':';
	private static final byte BULK_STRING_MARKER = '$';
	private static final byte ARRAY_MARKER = '*';

	private static final List<?> NIL_ARRAY = new ArrayList<>();
	private static final byte[] NIL_BULK_STRING = {};

	private final Charset charset;

	private final ByteBufQueue tempQueue;

	private byte parsing;
	private int remaining = -1;
	@Nullable
	private List<Integer> arraysRemaining;
	@Nullable
	private List<Object> arrayResult;

	public RESPv2Codec(ByteBufQueue tempQueue, Charset charset) {
		this.tempQueue = tempQueue;
		this.charset = charset;
	}

	@Override
	public ByteBuf encode(RedisCommand item) {
		Command command = item.getCommand();

		List<byte[]> arguments = Arrays.stream(command.name().split("_"))
				.map(part -> part.getBytes(charset))
				.collect(toList());

		arguments.addAll(item.getArguments());

		ByteBuf buf = ByteBufPool.allocate(estimateSize(arguments));

		buf.put(ARRAY_MARKER);
		buf.put(String.valueOf(arguments.size()).getBytes(charset));
		buf.put(CR_LF);

		for (byte[] argument : arguments) {
			buf.put(BULK_STRING_MARKER);
			buf.put(String.valueOf(argument.length).getBytes(charset));
			buf.put(CR_LF);
			buf.put(argument);
			buf.put(CR_LF);
		}

		return buf;
	}

	@Override
	public @Nullable RedisResponse tryDecode(ByteBufQueue bufs) throws ParseException {
		while (true) {
			if (bufs.isEmpty()) return null;
			if (parsing == 0) parsing = bufs.getByte();

			RedisResponse result = null;

			switch (parsing) {
				case STRING_MARKER:
					String string = decodeString(bufs, STRING_MAX_LEN);
					if (string != null) {
						result = addToArrayOr(string, RedisResponse::string);
					} else {
						return null;
					}
					break;
				case ERROR_MARKER:
					String message = decodeString(bufs, STRING_MAX_LEN);
					if (message != null) {
						ServerError error = new ServerError(message);
						result = addToArrayOr(error, RedisResponse::error);
					} else {
						return null;
					}
					break;
				case INTEGER_MARKER:
					String integer = decodeString(bufs, INTEGER_MAX_LEN);
					if (integer != null) {
						try {
							long value = Long.parseLong(integer);
							result = addToArrayOr(value, RedisResponse::integer);
						} catch (NumberFormatException e) {
							throw new ParseException(RESPv2Codec.class, "Malformed integer " + integer, e);
						}
					} else {
						return null;
					}
					break;
				case BULK_STRING_MARKER:
					byte[] bulkStringBytes = decodeBulkString(bufs);
					if (bulkStringBytes == NIL_BULK_STRING) {
						result = addToArrayOr(null, $ -> RedisResponse.nil());
					} else if (bulkStringBytes != null) {
						result = addToArrayOr(bulkStringBytes, RedisResponse::bytes);
					} else {
						return null;
					}
					break;
				case ARRAY_MARKER:
					int before = bufs.remainingBytes();
					List<?> array = decodeArray(bufs);
					if (array == NIL_ARRAY) {
						result = addToArrayOr(null, $ -> RedisResponse.nil());
					} else if (array != null) {
						result = addToArrayOr(array, RedisResponse::array);
					} else if (before == bufs.remainingBytes()) {
						// parsed nothing
						return null;
					}
					break;
				default:
					throw new ParseException(RESPv2Codec.class, "Unknown first byte '" + (char) parsing + "'");
			}

			if (result != null) return result;
		}
	}

	private static int estimateSize(List<byte[]> arguments) {
		return ARRAY_ESTIMATED_PADDING +
				ARG_ESTIMATED_PADDING * arguments.size() +
				arguments.stream().mapToInt(array -> array.length).sum();
	}

	@Nullable
	private String decodeString(ByteBufQueue bufs, int maxSize) throws ParseException {
		ByteBuf decoded = ofCrlfTerminatedBytes(maxSize + CR_LF_LENGTH).tryDecode(bufs);

		if (decoded != null) {
			tempQueue.add(decoded);
			return tempQueue.takeRemaining().asString(charset);
		}

		if (!bufs.isEmpty()) {
			tempQueue.add(bufs.takeExactSize(bufs.remainingBytes() - 1));
		}
		return null;
	}

	@Nullable
	private byte[] decodeBulkString(ByteBufQueue bufs) throws ParseException {
		if (remaining == -1) {
			Integer length = decodeLength(bufs);
			if (length == null) return null;
			if (length == -1) {
				parsing = 0;
				return NIL_BULK_STRING;
			}
			remaining = length;
		}

		ByteBuf result = ByteBufPool.allocate(min(bufs.remainingBytes(), remaining));
		remaining -= bufs.drainTo(result, remaining);
		tempQueue.add(result);

		if (remaining == 0) {
			if (!bufs.hasRemainingBytes(2)) {
				return null;
			} else {
				if (bufs.getByte() != CR || bufs.getByte() != LF) {
					throw new ParseException(RESPv2Codec.class, "Missing CR LF");
				}
				remaining = -1;
				return tempQueue.takeRemaining().asArray();
			}
		}
		return null;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private List<?> decodeArray(ByteBufQueue bufs) throws ParseException {
		Integer length = decodeLength(bufs);
		if (length == null) return null;
		parsing = 0;
		if (length == -1) return NIL_ARRAY;
		if (length != 0) {
			if (arraysRemaining == null) {
				arraysRemaining = new ArrayList<>();
				arrayResult = new ArrayList<>();
			} else {
				assert arrayResult != null;
				List<Object> array = arrayResult;
				for (int i = 0; i < arraysRemaining.size() - 1; i++) {
					array = (List<Object>) array.get(array.size() - 1);
				}

				array.add(new ArrayList<>());
			}
			arraysRemaining.add(length);
			return null;
		}
		return emptyList();

	}

	@Nullable
	private Integer decodeLength(ByteBufQueue bufs) throws ParseException {
		String numString = decodeString(bufs, INTEGER_MAX_LEN);
		if (numString == null) return null;

		int len;
		try {
			len = Integer.parseInt(numString);
		} catch (NumberFormatException e) {
			throw new ParseException(RESPv2Codec.class, "Malformed length: '" + numString + '\'', e);
		}

		if (len < -1) {
			throw new InvalidSizeException(RESPv2Codec.class, "Unsupported negative length: '" + len + '\'');
		}

		return len;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private <T> RedisResponse addToArrayOr(T value, Function<T, RedisResponse> fn) {
		parsing = 0;
		if (arrayResult == null) {
			assert arraysRemaining == null;
			return fn.apply(value);
		}
		assert arraysRemaining != null;

		List<Object> array = arrayResult;
		for (int i = 0; i < arraysRemaining.size() - 1; i++) {
			array = (List<Object>) array.get(array.size() - 1);
		}
		array.add(value);

		int index = arraysRemaining.size() - 1;
		while (true) {
			Integer remaining = arraysRemaining.get(index);
			if (remaining == 1) {
				arraysRemaining.remove(index--);
				if (arraysRemaining.isEmpty()) {
					List<?> arrayResult = this.arrayResult;
					this.arrayResult = null;
					this.arraysRemaining = null;
					return RedisResponse.array(arrayResult);
				}
			} else {
				arraysRemaining.set(index, remaining - 1);
				return null;
			}
		}
	}

}
