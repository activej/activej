package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.proto.calcite.RecordProjectionFnProto;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import static io.activej.serializer.BinarySerializers.BYTES_SERIALIZER;


public final class RecordProjectionFnSerializer implements BinarySerializer<RecordProjectionFn> {
	private final DefiningClassLoader classLoader;

	public RecordProjectionFnSerializer(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public RecordProjectionFnSerializer() {
		this(DefiningClassLoader.create());
	}

	@Override
	public void encode(BinaryOutput out, RecordProjectionFn recordProjectionFn) {
		RecordProjectionFnProto.RecordProjectionFn recorProjectionFnProto = RecordProjectionFnProto.RecordProjectionFn.newBuilder()
				.addAllFieldProjections(
						recordProjectionFn.getFieldProjections().stream()
								.map(fieldProjection -> {
									RecordProjectionFnProto.RecordProjectionFn.FieldProjection.Builder builder =
											RecordProjectionFnProto.RecordProjectionFn.FieldProjection.newBuilder();
									builder.setOperand(OperandConverter.convert(fieldProjection.operand()));
									if (fieldProjection.fieldName() == null) {
										builder.setIsNull(true);
									} else {
										builder.setValue(fieldProjection.fieldName());
									}
									return builder.build();
								})
								.toList())
				.build();

		BYTES_SERIALIZER.encode(out, recorProjectionFnProto.toByteArray());
	}

	@Override
	public RecordProjectionFn decode(BinaryInput in) throws CorruptedDataException {
		byte[] serialized = BYTES_SERIALIZER.decode(in);

		RecordProjectionFnProto.RecordProjectionFn recordProjectionFn;
		try {
			recordProjectionFn = RecordProjectionFnProto.RecordProjectionFn.parseFrom(serialized);
		} catch (InvalidProtocolBufferException e) {
			throw new CorruptedDataException(e.getMessage());
		}

		return RecordProjectionFn.create(
				recordProjectionFn.getFieldProjectionsList().stream()
						.map(fieldProjection -> new RecordProjectionFn.FieldProjection(
								OperandConverter.convert(classLoader, fieldProjection.getOperand()),
								switch (fieldProjection.getFieldNameCase()) {
									case VALUE -> fieldProjection.getValue();
									case IS_NULL -> null;
									case FIELDNAME_NOT_SET -> throw new IllegalArgumentException();
								}
						))
						.toList()
		);
	}

}
