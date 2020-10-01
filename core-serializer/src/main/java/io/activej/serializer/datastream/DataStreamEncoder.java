package io.activej.serializer.datastream;

import java.io.IOException;

public interface DataStreamEncoder<T> {
	void encode(DataOutputStreamEx stream, T item) throws IOException;
}
