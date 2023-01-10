package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.stream.EOSException;
import io.activej.serializer.stream.StreamDecoder;
import io.activej.serializer.stream.StreamEncoder;
import io.activej.serializer.stream.StreamInput;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

final class StreamByteBufsCodec<I, O> implements ByteBufsCodec<I, O> {
	private final StreamDecoder<I> input;
	private final StreamEncoder<O> output;

	StreamByteBufsCodec(StreamDecoder<I> input, StreamEncoder<O> output) {
		this.input = input;
		this.output = output;
	}

	@Override
	public ByteBuf encode(O item) {
		byte[] bytes = output.toByteArray(item);
		return ByteBuf.wrapForReading(bytes);
	}

	@Override
	public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
		ByteBuf buf = bufs.takeRemaining();

		BinaryInput binaryInput = new BinaryInput(buf.getArray());
		try (StreamInput streamInput = StreamInput.create(binaryInput)) {
			I decode;
			try {
				decode = input.decode(streamInput);
			} catch (EOSException e) {
				bufs.add(buf);
				return null;
			}
			buf.moveHead(binaryInput.pos());
			bufs.add(buf);
			return decode;
		} catch (IOException e) {
			throw new MalformedDataException(e);
		}
	}
}
