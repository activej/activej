package io.activej.csp.binary.codec.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.codec.ByteBufsCodec;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

import java.util.function.UnaryOperator;

@ExposedInternals
public final class OfDelimiter implements ByteBufsCodec<ByteBuf, ByteBuf> {
	public final UnaryOperator<ByteBuf> delimiterOut;
	public final ByteBufsDecoder<ByteBuf> delimiterIn;

	public OfDelimiter(UnaryOperator<ByteBuf> delimiterOut, ByteBufsDecoder<ByteBuf> delimiterIn) {
		this.delimiterOut = delimiterOut;
		this.delimiterIn = delimiterIn;
	}

	@Override
	public ByteBuf encode(ByteBuf buf) {
		return delimiterOut.apply(buf);
	}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		return delimiterIn.tryDecode(bufs);
	}
}
