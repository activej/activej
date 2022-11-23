package io.activej.dataflow.graph;

import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.serializer.BinarySerializer;

public interface StreamSchema<T> {
	Class<T> createClass();

	BinarySerializer<T> createSerializer(BinarySerializerLocator locator);
}
