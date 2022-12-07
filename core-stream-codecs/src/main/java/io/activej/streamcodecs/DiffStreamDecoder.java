package io.activej.streamcodecs;

import java.io.IOException;

public interface DiffStreamDecoder<T> {
	T decodeDiff(StreamInput input, T from) throws IOException;
}
