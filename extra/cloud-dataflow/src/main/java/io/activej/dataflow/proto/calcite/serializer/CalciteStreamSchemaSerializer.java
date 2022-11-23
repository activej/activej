package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.RecordStreamSchema;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.proto.calcite.CalciteStreamSchemaProto.CalciteStreamSchema;
import io.activej.dataflow.proto.serializer.CustomStreamSchemaSerializer;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.CorruptedDataException;

import static io.activej.serializer.BinarySerializers.BYTES_SERIALIZER;

public final class CalciteStreamSchemaSerializer implements CustomStreamSchemaSerializer {
	private final DefiningClassLoader classLoader;

	public CalciteStreamSchemaSerializer(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public CalciteStreamSchemaSerializer() {
		this.classLoader = DefiningClassLoader.create();
	}

	@Override
	public void encode(BinaryOutput out, StreamSchema<?> streamSchema) {
		byte[] item = convert(streamSchema).toByteArray();
		BYTES_SERIALIZER.encode(out, item);
	}

	@Override
	public StreamSchema<?> decode(BinaryInput in) throws CorruptedDataException {
		byte[] serialized = BYTES_SERIALIZER.decode(in);
		CalciteStreamSchema calciteStreamSchema;
		try {
			calciteStreamSchema = CalciteStreamSchema.parseFrom(serialized);
		} catch (InvalidProtocolBufferException e) {
			throw new CorruptedDataException(e.getMessage());
		}

		return convert(calciteStreamSchema);
	}

	public StreamSchema<?> convert(CalciteStreamSchema streamSchema) {
		return switch (streamSchema.getStreamSchemaCase()) {
			case RECORD_STREAM_SCHEMA -> {
				CalciteStreamSchema.RecordStreamSchema recordStreamSchema = streamSchema.getRecordStreamSchema();
				RecordScheme recordScheme = RecordSchemeConverter
						.convert(classLoader, recordStreamSchema.getRecordScheme())
						.build();
				yield RecordStreamSchema.create(recordScheme);
			}
			case STREAMSCHEMA_NOT_SET -> throw new CorruptedDataException("Calcite stream schema was not set");
		};
	}

	public static CalciteStreamSchema convert(StreamSchema<?> streamSchema) {
		CalciteStreamSchema.Builder builder = CalciteStreamSchema.newBuilder();

		if (streamSchema instanceof RecordStreamSchema recordStreamSchema) {
			builder.setRecordStreamSchema(
					CalciteStreamSchema.RecordStreamSchema.newBuilder()
							.setRecordScheme(RecordSchemeConverter.convert(recordStreamSchema.getRecordScheme()))
			);
		} else {
			throw new IllegalArgumentException("Unsupported calcite stream schema type: " + streamSchema.getClass().getName());
		}

		return builder.build();
	}
}
