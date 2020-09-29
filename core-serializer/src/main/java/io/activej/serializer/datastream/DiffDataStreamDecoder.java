package io.activej.serializer.datastream;

import java.io.IOException;

public interface DiffDataStreamDecoder<T> {
	T decodeDiff(DataInputStreamEx stream, T from) throws IOException;
}
