package io.activej.serializer.datastream;

import java.io.IOException;

public interface DataStreamDecoder<T> {
	T decode(DataInputStreamEx stream) throws IOException;
}
