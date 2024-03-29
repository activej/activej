package io.activej.dataflow.calcite;

import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.BinarySerializers;
import io.activej.serializer.util.RecordBinarySerializer;
import io.activej.types.Primitives;

import java.lang.reflect.Type;
import java.util.List;

public final class RecordStreamSchema implements StreamSchema<Record> {
	private final RecordScheme recordScheme;

	private RecordStreamSchema(RecordScheme recordScheme) {
		this.recordScheme = recordScheme;
	}

	public static RecordStreamSchema create(RecordScheme recordScheme) {
		return new RecordStreamSchema(recordScheme);
	}

	@Override
	public Class<Record> createClass() {
		return Record.class;
	}

	@Override
	public BinarySerializer<Record> createSerializer(BinarySerializerModule.BinarySerializerLocator locator) {
		RecordBinarySerializer.Builder recordSerializerBuilder = RecordBinarySerializer.builder(recordScheme);

		List<String> fields = recordScheme.getFields();
		List<Type> types = recordScheme.getTypes();
		for (int i = 0; i < fields.size(); i++) {
			Type type = types.get(i);
			BinarySerializer<Object> fieldSerializer = locator.get(type);
			if (!Primitives.isPrimitiveType(type)) {
				fieldSerializer = BinarySerializers.ofNullable(fieldSerializer);
			}
			recordSerializerBuilder.withField(fields.get(i), fieldSerializer);
		}

		return recordSerializerBuilder.build();
	}

	public RecordScheme getRecordScheme() {
		return recordScheme;
	}

	@Override
	public String toString() {
		return "RecordStreamSchema{recordScheme=" + recordScheme + '}';
	}
}
