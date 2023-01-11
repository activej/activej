package io.activej.dataflow.calcite;

import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.BinarySerializers;
import io.activej.serializer.util.BinarySerializer_Record;
import io.activej.types.Primitives;

import java.lang.reflect.Type;
import java.util.List;

public final class StreamSchema_Record implements StreamSchema<Record> {
	private final RecordScheme recordScheme;

	private StreamSchema_Record(RecordScheme recordScheme) {
		this.recordScheme = recordScheme;
	}

	public static StreamSchema_Record create(RecordScheme recordScheme) {
		return new StreamSchema_Record(recordScheme);
	}

	@Override
	public Class<Record> createClass() {
		return Record.class;
	}

	@Override
	public BinarySerializer<Record> createSerializer(BinarySerializerModule.BinarySerializerLocator locator) {
		BinarySerializer_Record recordSerializer = BinarySerializer_Record.create(recordScheme);

		List<String> fields = recordScheme.getFields();
		List<Type> types = recordScheme.getTypes();
		for (int i = 0; i < fields.size(); i++) {
			Type type = types.get(i);
			BinarySerializer<Object> fieldSerializer = locator.get(type);
			if (!Primitives.isPrimitiveType(type)) {
				fieldSerializer = BinarySerializers.ofNullable(fieldSerializer);
			}
			recordSerializer.withField(fields.get(i), fieldSerializer);
		}

		return recordSerializer;
	}

	public RecordScheme getRecordScheme() {
		return recordScheme;
	}

	@Override
	public String toString() {
		return "RecordStreamSchema{" +
				"recordScheme=" + recordScheme +
				'}';
	}
}
