package io.activej.streamcodecs;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public interface StreamDecoder<T> {
	T decode(StreamInput input) throws IOException;

	default T fromByteArray(byte[] byteArray) {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(byteArray)) {
			try (StreamInput streamInput = StreamInput.create(bais)) {
				return decode(streamInput);
			}
		} catch (IOException e) {
			throw new AssertionError("IO exception should not happen", e);
		}
	}
}
