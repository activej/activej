package io.activej.streamcodecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public interface StreamEncoder<T> {
	void encode(StreamOutput output, T item) throws IOException;

	default byte[] toByteArray(T item) {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (StreamOutput streamOutput = StreamOutput.create(baos)) {
				encode(streamOutput, item);
			}
			return baos.toByteArray();
		} catch (IOException e) {
			throw new AssertionError("IO exception should not happen", e);
		}
	}
}
