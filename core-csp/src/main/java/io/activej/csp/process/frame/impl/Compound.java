package io.activej.csp.process.frame.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.process.frame.BlockDecoder;
import io.activej.csp.process.frame.BlockEncoder;
import io.activej.csp.process.frame.FrameFormat;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.List;

@ExposedInternals
public final class Compound implements FrameFormat {
	public final List<FrameFormat> formats;

	public Compound(List<FrameFormat> formats) {
		this.formats = formats;
	}

	@Override
	public BlockEncoder createEncoder() {
		return formats.get(0).createEncoder();
	}

	@Override
	public BlockDecoder createDecoder() {
		return new BlockDecoder() {
			BlockDecoder decoder;
			BlockDecoder possibleDecoder;
			Iterator<FrameFormat> possibleDecoders = formats.iterator();

			@Override
			public void reset() {
				if (decoder != null) {
					decoder.reset();
				}
			}

			@Override
			public boolean ignoreMissingEndOfStreamBlock() {
				if (decoder != null) return decoder.ignoreMissingEndOfStreamBlock();

				// rare case of empty stream
				return formats.stream().map(FrameFormat::createDecoder).anyMatch(BlockDecoder::ignoreMissingEndOfStreamBlock);
			}

			@Override
			public @Nullable ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
				if (decoder != null) return decoder.decode(bufs);
				return tryNextDecoder(bufs);
			}

			private ByteBuf tryNextDecoder(ByteBufs bufs) throws MalformedDataException {
				while (true) {
					if (possibleDecoder == null) {
						if (!possibleDecoders.hasNext()) throw new UnknownFormatException();
						possibleDecoder = possibleDecoders.next().createDecoder();
					}

					try {
						int bytesBeforeDecoding = bufs.remainingBytes();
						ByteBuf buf = possibleDecoder.decode(bufs);
						if (buf != null || bytesBeforeDecoding != bufs.remainingBytes()) {
							decoder = possibleDecoder;
							possibleDecoders = null;
						}
						return buf;
					} catch (MalformedDataException ignored) {
					}

					possibleDecoder = null;
				}
			}
		};
	}
}
