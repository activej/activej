package io.activej.dataflow.proto.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.where.*;
import io.activej.serializer.CorruptedDataException;

final class OperandConverters {
	public static OperandProto.Operand convert(Operand operand) {
		OperandProto.Operand.Builder builder = OperandProto.Operand.newBuilder();

		if (operand instanceof OperandRecordField operandRecordField) {
			builder.setRecordField(
					OperandProto.Operand.RecordField.newBuilder()
							.setIndex(operandRecordField.getIndex())
			);
		} else if (operand instanceof OperandScalar operandScalar) {
			OperandProto.Operand.Scalar.Builder scalarBuilder = OperandProto.Operand.Scalar.newBuilder();

			Object value = operandScalar.getValue().getValue();
			if (value == null) {
				scalarBuilder.setNull(OperandProto.Operand.Scalar.None.newBuilder());
			} else if (value instanceof Integer anInteger) {
				scalarBuilder.setInteger(anInteger);
			} else if (value instanceof Long aLong) {
				scalarBuilder.setLong(aLong);
			} else if (value instanceof Float aFloat) {
				scalarBuilder.setFloat(aFloat);
			} else if (value instanceof Double aDouble) {
				scalarBuilder.setDouble(aDouble);
			} else if (value instanceof Boolean aBoolean) {
				scalarBuilder.setBoolean(aBoolean);
			} else if (value instanceof String aString) {
				scalarBuilder.setString(aString);
			} else {
				throw new IllegalArgumentException("Unsupported scalar type: " + value.getClass());
			}

			builder.setScalar(scalarBuilder);
		} else if (operand instanceof OperandMapGet operandMapGet) {
			builder.setMapGet(
					OperandProto.Operand.MapGet.newBuilder()
							.setMapOperand(convert(operandMapGet.getMapOperand()))
							.setKeyOperand(convert(operandMapGet.getKeyOperand()))
			);
		} else if (operand instanceof OperandListGet operandListGet) {
			builder.setListGet(
					OperandProto.Operand.ListGet.newBuilder()
							.setListOperand(convert(operandListGet.getListOperand()))
							.setIndexOperand(convert(operandListGet.getIndexOperand()))
			);
		} else if (operand instanceof OperandFieldAccess operandFieldAccess) {
			builder.setFieldGet(
					OperandProto.Operand.FieldGet.newBuilder()
							.setObjectOperand(convert(operandFieldAccess.getObjectOperand()))
							.setFieldName(operandFieldAccess.getFieldName())
			);
		} else if (operand instanceof OperandIfNull operandIfNull) {
			builder.setIfNull(
					OperandProto.Operand.IfNull.newBuilder()
							.setCheckedOperand(convert(operandIfNull.getCheckedOperand()))
							.setDefaultValueOperand(convert(operandIfNull.getDefaultValueOperand()))
			);
		} else {
			throw new IllegalArgumentException("Unknown operand type: " + operand.getClass());
		}

		return builder.build();
	}

	public static Operand convert(DefiningClassLoader classLoader, OperandProto.Operand operand) {
		return switch (operand.getOperandCase()) {
			case RECORD_FIELD ->
					new OperandRecordField(operand.getRecordField().getIndex());
			case SCALAR -> new OperandScalar(
					switch (operand.getScalar().getValueCase()) {
						case NULL -> null;
						case INTEGER -> Value.materializedValue(int.class, operand.getScalar().getInteger());
						case LONG -> Value.materializedValue(long.class, operand.getScalar().getLong());
						case FLOAT -> Value.materializedValue(float.class, operand.getScalar().getFloat());
						case DOUBLE -> Value.materializedValue(double.class, operand.getScalar().getDouble());
						case BOOLEAN -> Value.materializedValue(boolean.class, operand.getScalar().getBoolean());
						case STRING -> Value.materializedValue(String.class, operand.getScalar().getString());
						case VALUE_NOT_SET -> throw new CorruptedDataException("Scalar value not set");
					}
			);
			case MAP_GET -> new OperandMapGet<>(
					convert(classLoader, operand.getMapGet().getMapOperand()),
					convert(classLoader, operand.getMapGet().getKeyOperand())
			);
			case LIST_GET -> new OperandListGet(
					convert(classLoader, operand.getListGet().getListOperand()),
					convert(classLoader, operand.getListGet().getIndexOperand())
			);
			case FIELD_GET -> new OperandFieldAccess(
					convert(classLoader, operand.getFieldGet().getObjectOperand()),
					operand.getFieldGet().getFieldName(),
					classLoader
			);
			case IF_NULL -> new OperandIfNull(
					convert(classLoader, operand.getIfNull().getCheckedOperand()),
					convert(classLoader, operand.getIfNull().getDefaultValueOperand())
			);
			case OPERAND_NOT_SET -> throw new CorruptedDataException("Operand not set");
		};
	}
}
