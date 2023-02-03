package io.activej.csp.process.frame.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.csp.process.frame.BlockDecoder;
import io.activej.csp.process.frame.BlockEncoder;
import io.activej.csp.process.frame.FrameFormat;

@ExposedInternals
public final class MagicNumberAdapter implements FrameFormat {
	public final FrameFormat peerFormat;
	public final byte[] magicNumber;

	private final ByteBufsDecoder<byte[]> magicNumberValidator;

	public MagicNumberAdapter(FrameFormat peerFormat, byte[] magicNumber) {
		this.peerFormat = peerFormat;
		this.magicNumber = magicNumber;
		this.magicNumberValidator = ByteBufsDecoder.assertBytes(magicNumber);
	}

	@Override
	public BlockEncoder createEncoder() {
		return new BlockEncoder() {
			final BlockEncoder peer = peerFormat.createEncoder();

			boolean writeMagicNumber = true;

			@Override
			public ByteBuf encode(ByteBuf inputBuf) {
				ByteBuf peerEncoded = peer.encode(inputBuf);
				if (writeMagicNumber) {
					writeMagicNumber = false;
					return ByteBufPool.append(ByteBuf.wrapForReading(magicNumber), peerEncoded);
				}
				return peerEncoded;
			}

			@Override
			public void reset() {
				writeMagicNumber = true;
			}

			@Override
			public ByteBuf encodeEndOfStreamBlock() {
				ByteBuf peerEncodedEndOfStream = peer.encodeEndOfStreamBlock();
				if (writeMagicNumber) {
					writeMagicNumber = false;
					return ByteBufPool.append(ByteBuf.wrapForReading(magicNumber), peerEncodedEndOfStream);
				}
				return peerEncodedEndOfStream;
			}
		};
	}

	@Override
	public BlockDecoder createDecoder() {
		return new BlockDecoder() {
			final BlockDecoder peer = peerFormat.createDecoder();

			boolean validateMagicNumber = true;

			@Override
			public ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
				if (validateMagicNumber) {
					if (magicNumberValidator.tryDecode(bufs) == null) return null;
					validateMagicNumber = false;
				}
				return peer.decode(bufs);
			}

			@Override
			public void reset() {
				validateMagicNumber = true;
			}

			@Override
			public boolean ignoreMissingEndOfStreamBlock() {
				return peer.ignoreMissingEndOfStreamBlock();
			}

		};
	}
}
