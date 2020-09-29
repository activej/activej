package io.activej.serializer.datastream;

import java.io.IOException;

public interface DiffDataStreamEncoder<T> {
	void encodeDiff(DataOutputStreamEx stream, T from, T to) throws IOException;
}
