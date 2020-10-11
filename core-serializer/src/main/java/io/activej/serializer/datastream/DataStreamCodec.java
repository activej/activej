package io.activej.serializer.datastream;

import java.io.IOException;

public interface DataStreamCodec<T> extends DataStreamEncoder<T>, DataStreamDecoder<T> {

	static <T> DataStreamCodec<T> of(DataStreamEncoder<? super T> encoder, DataStreamDecoder<? extends T> decoder) {
		return new DataStreamCodec<T>() {
			@Override
			public void encode(DataOutputStreamEx stream, T item) throws IOException {
				encoder.encode(stream, item);
			}

			@Override
			public T decode(DataInputStreamEx stream) throws IOException {
				return decoder.decode(stream);
			}
		};
	}
}
