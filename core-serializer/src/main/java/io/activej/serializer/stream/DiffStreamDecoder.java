package io.activej.serializer.stream;

import java.io.IOException;

public interface DiffStreamDecoder<T> extends StreamDecoder<T> {
	T decodeDiff(StreamInput input, T from) throws IOException;
}
