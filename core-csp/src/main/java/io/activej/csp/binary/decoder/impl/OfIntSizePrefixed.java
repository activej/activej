package io.activej.csp.binary.decoder.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.Utils;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public class OfIntSizePrefixed implements ByteBufsDecoder<ByteBuf> {
	public final Utils.IntByteScanner scanner = new Utils.IntByteScanner();

	public final int maxSize;

	public OfIntSizePrefixed(int maxSize) {
		this.maxSize = maxSize;
	}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		if (bufs.scanBytes(scanner) == 0) return null;

		int size = scanner.getValue();

		if (size < 0)
			throw new InvalidSizeException("Invalid size of bytes to be read, should be greater than 0");
		if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");

		if (!bufs.hasRemainingBytes(4 + size)) return null;
		bufs.skip(4);
		return bufs.takeExactSize(size);
	}
}
