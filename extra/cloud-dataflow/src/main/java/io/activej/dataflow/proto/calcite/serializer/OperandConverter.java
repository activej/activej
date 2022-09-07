package io.activej.dataflow.proto.calcite.serializer;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.operand.*;
import io.activej.dataflow.proto.calcite.OperandProto;
import io.activej.serializer.CorruptedDataException;

import static java.util.stream.Collectors.toList;

final class OperandConverter {

	private OperandConverter() {
	}

	static OperandProto.Operand convert(Operand<?> operand) {
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
		} else if (operand instanceof OperandFieldAccess operandFieldAccess) {
			builder.setFieldGet(
					OperandProto.Operand.FieldGet.newBuilder()
							.setObjectOperand(convert(operandFieldAccess.getObjectOperand()))
							.setFieldName(operandFieldAccess.getFieldName())
			);
		} else if (operand instanceof OperandFunction<?> operandFunction) {
			builder.setFunction(
					OperandProto.Operand.Function.newBuilder()
							.setFunctionName(operandFunction.getClass().getName())
							.addAllOperands(operandFunction.getOperands().stream()
									.map(OperandConverter::convert)
									.toList())
			);
		} else {
			throw new IllegalArgumentException("Unknown operand type: " + operand.getClass());
		}

		return builder.build();
	}

	static Operand<?> convert(DefiningClassLoader classLoader, OperandProto.Operand operand) {
		return switch (operand.getOperandCase()) {
			case RECORD_FIELD -> new OperandRecordField(operand.getRecordField().getIndex());
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
			case FIELD_GET -> new OperandFieldAccess(
					convert(classLoader, operand.getFieldGet().getObjectOperand()),
					operand.getFieldGet().getFieldName(),
					classLoader
			);
			case FUNCTION -> {
				OperandProto.Operand.Function function = operand.getFunction();
				String functionName = function.getFunctionName();
				OperandFunctionRegistry.OperandFunctionFactory<?> factory = OperandFunctionRegistry.getFactory(functionName);
				if (factory == null) {
					throw new CorruptedDataException("Unknown function: " + functionName);
				}
				yield factory.create(
						function.getOperandsList().stream()
								.map(functionOperand -> convert(classLoader, functionOperand))
								.collect(toList())
				);
			}
			case OPERAND_NOT_SET -> throw new CorruptedDataException("Operand not set");
		};
	}
}
