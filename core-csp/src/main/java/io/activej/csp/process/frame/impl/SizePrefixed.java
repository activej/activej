package io.activej.csp.process.frame.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.process.frame.BlockDecoder;
import io.activej.csp.process.frame.BlockEncoder;
import io.activej.csp.process.frame.FrameFormat;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class SizePrefixed implements FrameFormat {
	private static final byte[] ZERO_BYTE_ARRAY = {0};

	@Override
	public BlockEncoder createEncoder() {
		return new BlockEncoder() {
			@Override
			public ByteBuf encode(ByteBuf inputBuf) {
				int len = inputBuf.readRemaining();
				ByteBuf outputBuf = ByteBufPool.allocate(len + 5);
				outputBuf.writeVarInt(len);
				outputBuf.put(inputBuf);
				return outputBuf;
			}

			@Override
			public void reset() {
			}

			@Override
			public ByteBuf encodeEndOfStreamBlock() {
				return ByteBuf.wrapForReading(ZERO_BYTE_ARRAY);
			}
		};
	}

	@Override
	public BlockDecoder createDecoder() {
		return new BlockDecoder() {
			private final LengthByteScanner lengthScanner = new LengthByteScanner();

			@Override
			public @Nullable ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
				int bytes = bufs.scanBytes(lengthScanner);
				if (bytes == 0) return null;
				int length = lengthScanner.value;
				if (length == 0) {
					bufs.skip(bytes);
					return END_OF_STREAM;
				}
				if (!bufs.hasRemainingBytes(bytes + length)) return null;
				bufs.skip(bytes);
				return bufs.takeExactSize(length);
			}

			@Override
			public void reset() {
			}

			@Override
			public boolean ignoreMissingEndOfStreamBlock() {
				return false;
			}
		};
	}

	public static final class LengthByteScanner implements ByteBufs.ByteScanner {
		int value;

		@Override
		public boolean consume(int index, byte b) throws MalformedDataException {
			value = index == 0 ? b & 0x7F : value | (b & 0x7F) << index * 7;
			if (b >= 0) {
				if (value < 0) throw new InvalidSizeException("Negative length");
				return true;
			}
			if (index == 4) throw new InvalidSizeException("Could not read var int");
			return false;
		}
	}
}
