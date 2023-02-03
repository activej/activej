package io.activej.csp.process.frame.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.process.frame.BlockDecoder;
import io.activej.csp.process.frame.BlockEncoder;
import io.activej.csp.process.frame.FrameFormat;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class Identity implements FrameFormat {
	@Override
	public BlockEncoder createEncoder() {
		return new BlockEncoder() {
			@Override
			public ByteBuf encode(ByteBuf inputBuf) {
				return inputBuf.slice();
			}

			@Override
			public void reset() {
			}

			@Override
			public ByteBuf encodeEndOfStreamBlock() {
				return ByteBuf.empty();
			}
		};
	}

	@Override
	public BlockDecoder createDecoder() {
		return new BlockDecoder() {

			@Override
			public @Nullable ByteBuf decode(ByteBufs bufs) {
				return bufs.hasRemaining() ? bufs.takeRemaining() : null;
			}

			@Override
			public void reset() {
			}

			@Override
			public boolean ignoreMissingEndOfStreamBlock() {
				return true;
			}
		};
	}
}
