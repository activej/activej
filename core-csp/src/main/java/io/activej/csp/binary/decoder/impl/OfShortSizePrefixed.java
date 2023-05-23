package io.activej.csp.binary.decoder.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public class OfShortSizePrefixed implements ByteBufsDecoder<ByteBuf> {
	public final int maxSize;

	public OfShortSizePrefixed(int maxSize) {this.maxSize = maxSize;}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		if (!bufs.hasRemainingBytes(2)) return null;
		int size = (bufs.peekByte(0) & 0xFF) << 8
			| (bufs.peekByte(1) & 0xFF);
		if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");
		if (!bufs.hasRemainingBytes(2 + size)) return null;
		bufs.skip(2);
		return bufs.takeExactSize(size);
	}
}
