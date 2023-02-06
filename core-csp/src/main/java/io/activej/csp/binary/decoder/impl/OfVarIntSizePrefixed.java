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
public final class OfVarIntSizePrefixed implements ByteBufsDecoder<ByteBuf> {
	public final Utils.VarIntByteScanner scanner;
	public final int maxSize;

	public OfVarIntSizePrefixed(int maxSize) {
		this.maxSize = maxSize;
		scanner = new Utils.VarIntByteScanner();
	}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		int bytes = bufs.scanBytes(scanner);

		if (bytes == 0) return null;

		int size = scanner.getResult();

		if (size < 0)
			throw new InvalidSizeException("Invalid size of bytes to be read, should be greater than 0");
		if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");

		if (!bufs.hasRemainingBytes(bytes + size)) return null;
		bufs.skip(bytes);
		return bufs.takeExactSize(size);
	}
}
