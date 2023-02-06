package io.activej.csp.binary.decoder.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public class OfByteSizePrefixed implements ByteBufsDecoder<ByteBuf> {
	public final int maxSize;

	public OfByteSizePrefixed(int maxSize) {this.maxSize = maxSize;}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		if (!bufs.hasRemaining()) return null;
		int size = bufs.peekByte() & 0xFF;
		if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");
		if (!bufs.hasRemainingBytes(1 + size)) return null;
		bufs.skip(1);
		return bufs.takeExactSize(size);
	}
}
