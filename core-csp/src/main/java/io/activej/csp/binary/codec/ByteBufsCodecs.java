package io.activej.csp.binary.codec;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.annotation.StaticFactories;
import io.activej.csp.binary.codec.impl.OfDelimiter;
import io.activej.csp.binary.codec.impl.OfStreamCodecs;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.activej.csp.binary.decoder.ByteBufsDecoders;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamDecoder;
import io.activej.serializer.stream.StreamEncoder;

import java.util.function.UnaryOperator;

@StaticFactories(ByteBufsCodec.class)
public class ByteBufsCodecs {
	public static ByteBufsCodec<ByteBuf, ByteBuf> ofDelimiter(ByteBufsDecoder<ByteBuf> delimiterIn, UnaryOperator<ByteBuf> delimiterOut) {
		return new OfDelimiter(delimiterOut, delimiterIn);
	}

	public static ByteBufsCodec<ByteBuf, ByteBuf> nullTerminated() {
		return ByteBufsCodecs
			.ofDelimiter(
				ByteBufsDecoders.ofNullTerminatedBytes(),
				buf -> {
					ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
					buf1.put((byte) 0);
					return buf1;
				});
	}

	public static <I, O> ByteBufsCodec<I, O> ofStreamCodecs(StreamDecoder<I> decoder, StreamEncoder<O> encoder) {
		return new OfStreamCodecs<>(decoder, encoder);
	}

	public static <T> ByteBufsCodec<T, T> ofStreamCodecs(StreamCodec<T> codec) {
		return ofStreamCodecs(codec, codec);
	}

}
