package io.activej.csp.binary.decoder.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;

@ExposedInternals
public class OfCrlfTerminated implements ByteBufsDecoder<ByteBuf> {
	public final int maxSize;

	public OfCrlfTerminated(int maxSize) {this.maxSize = maxSize;}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		int bytes = bufs.scanBytes(new ByteBufs.ByteScanner() {
			boolean crFound;

			@Override
			public boolean consume(int index, byte b) throws MalformedDataException {
				if (crFound) {
					if (b == LF) {
						return true;
					}
					crFound = false;
				}
				if (index == maxSize - 1) {
					throw new MalformedDataException("No CRLF is found in " + maxSize + " bytes");
				}
				if (b == CR) {
					crFound = true;
				}
				return false;
			}
		});

		if (bytes == 0) return null;

		ByteBuf buf = bufs.takeExactSize(bytes);
		buf.moveTail(-2);
		return buf;
	}
}
