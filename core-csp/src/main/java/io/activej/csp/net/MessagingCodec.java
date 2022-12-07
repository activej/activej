package io.activej.csp.net;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamEncoder;
import io.activej.streamcodecs.StreamInput;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

public final class MessagingCodec<I, O> implements ByteBufsCodec<I, O> {
	private final StreamCodec<I> inputCodec;
	private final StreamEncoder<O> outputCodec;

	private MessagingCodec(StreamCodec<I> inputCodec, StreamEncoder<O> outputCodec) {
		this.inputCodec = inputCodec;
		this.outputCodec = outputCodec;
	}

	public static <I, O> MessagingCodec<I, O> create(StreamCodec<I> inputCodec, StreamEncoder<O> outputCodec) {
		return new MessagingCodec<>(inputCodec, outputCodec);
	}

	public static <T> MessagingCodec<T, T> create(StreamCodec<T> codec) {
		return new MessagingCodec<>(codec, codec);
	}

	@Override
	public ByteBuf encode(O item) {
		byte[] bytes = outputCodec.toByteArray(item);
		return ByteBuf.wrapForReading(bytes);
	}

	@Override
	public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
		ByteBuf buf = bufs.takeRemaining();
		try (ByteArrayInputStream bais = new ByteArrayInputStream(buf.getArray())) {
			try (StreamInput streamInput = StreamInput.create(bais)) {
				I decode;
				try {
					decode = inputCodec.decode(streamInput);
				} catch (EOFException e) {
					bufs.add(buf);
					return null;
				}
				buf.moveHead(streamInput.pos());
				bufs.add(buf);
				return decode;
			}
		} catch (IOException e) {
			throw new MalformedDataException(e);
		}
	}
}
