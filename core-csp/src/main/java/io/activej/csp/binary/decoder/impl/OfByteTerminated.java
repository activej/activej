package io.activej.csp.binary.decoder.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public class OfByteTerminated implements ByteBufsDecoder<ByteBuf> {
	public final byte terminator;
	public final int maxSize;

	public OfByteTerminated(byte terminator, int maxSize) {
		this.terminator = terminator;
		this.maxSize = maxSize;
	}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		int bytes = bufs.scanBytes((index, nextByte) -> {
			if (nextByte == terminator) {
				return true;
			}
			if (index == maxSize - 1) {
				throw new MalformedDataException("No terminator byte is found in " + maxSize + " bytes");
			}
			return false;
		});

		if (bytes == 0) return null;

		ByteBuf buf = bufs.takeExactSize(bytes);
		buf.moveTail(-1);
		return buf;
	}
}
