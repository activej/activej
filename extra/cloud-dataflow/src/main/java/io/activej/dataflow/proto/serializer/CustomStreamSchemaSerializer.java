package io.activej.dataflow.proto.serializer;

import io.activej.dataflow.graph.StreamSchema;
import io.activej.serializer.BinarySerializer;

public interface CustomStreamSchemaSerializer extends BinarySerializer<StreamSchema<?>> {
}
