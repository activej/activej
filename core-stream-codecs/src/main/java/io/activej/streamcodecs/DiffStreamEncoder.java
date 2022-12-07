package io.activej.streamcodecs;

import java.io.IOException;

public interface DiffStreamEncoder<T> {
	void encodeDiff(StreamOutput output, T from, T to) throws IOException;
}
