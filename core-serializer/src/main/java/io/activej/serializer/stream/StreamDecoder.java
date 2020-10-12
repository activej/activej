package io.activej.serializer.stream;

import java.io.IOException;

public interface StreamDecoder<T> {
	T decode(StreamInput input) throws IOException;
}
