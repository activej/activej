package io.activej.csp.binary.decoder;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.annotation.StaticFactories;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.impl.*;

import static io.activej.bytebuf.ByteBufStrings.CR;

@StaticFactories(ByteBufsDecoder.class)
public class ByteBufsDecoders {

	public static ByteBufsDecoder<byte[]> assertBytes(byte[] data) {
		return bufs -> bufs.consumeBytes((index, b) -> {
			if (b != data[index]) {
				throw new MalformedDataException(
					"Array of bytes differs at index " + index +
					"[Expected: " + data[index] + ", actual: " + b + ']');
			}
			return index == data.length - 1;
		}) != 0 ? data : null;
	}

	public static ByteBufsDecoder<ByteBuf> ofFixedSize(int length) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(length)) return null;
			return bufs.takeExactSize(length);
		};
	}

	public static ByteBufsDecoder<ByteBuf> ofNullTerminatedBytes() {
		return ofNullTerminatedBytes(Integer.MAX_VALUE);
	}

	public static ByteBufsDecoder<ByteBuf> ofNullTerminatedBytes(int maxSize) {
		return new OfByteTerminated((byte) 0, maxSize);
	}

	public static ByteBufsDecoder<ByteBuf> ofCrTerminatedBytes() {
		return ofCrTerminatedBytes(Integer.MAX_VALUE);
	}

	public static ByteBufsDecoder<ByteBuf> ofCrTerminatedBytes(int maxSize) {
		return new OfByteTerminated(CR, maxSize);
	}

	public static ByteBufsDecoder<ByteBuf> ofCrlfTerminatedBytes() {
		return ofCrlfTerminatedBytes(Integer.MAX_VALUE);
	}

	public static ByteBufsDecoder<ByteBuf> ofCrlfTerminatedBytes(int maxSize) {
		return new OfCrlfTerminated(maxSize);
	}

	public static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes() {
		return ofIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

	public static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes(int maxSize) {
		return new OfIntSizePrefixed(maxSize);
	}

	public static ByteBufsDecoder<ByteBuf> ofShortSizePrefixedBytes() {
		return ofShortSizePrefixedBytes(Integer.MAX_VALUE);
	}

	public static ByteBufsDecoder<ByteBuf> ofShortSizePrefixedBytes(int maxSize) {
		return new OfShortSizePrefixed(maxSize);
	}

	public static ByteBufsDecoder<ByteBuf> ofByteSizePrefixedBytes() {
		return ofByteSizePrefixedBytes(Integer.MAX_VALUE);
	}

	public static ByteBufsDecoder<ByteBuf> ofByteSizePrefixedBytes(int maxSize) {
		return new OfByteSizePrefixed(maxSize);
	}

	public static ByteBufsDecoder<ByteBuf> ofVarIntSizePrefixedBytes() {
		return ofVarIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

	public static ByteBufsDecoder<ByteBuf> ofVarIntSizePrefixedBytes(int maxSize) {
		return new OfVarIntSizePrefixed(maxSize);
	}
}
