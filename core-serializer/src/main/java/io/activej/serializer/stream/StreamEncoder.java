package io.activej.serializer.stream;

import java.io.IOException;

public interface StreamEncoder<T> {
	void encode(StreamOutput output, T item) throws IOException;
}
