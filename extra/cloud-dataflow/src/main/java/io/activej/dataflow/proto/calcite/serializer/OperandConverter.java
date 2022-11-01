package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.ByteString;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.operand.*;
import io.activej.dataflow.proto.calcite.OperandProto;
import io.activej.serializer.CorruptedDataException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

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
			} else if (value instanceof BigDecimal bigDecimal) {
				scalarBuilder.setBigDecimal(
						OperandProto.Operand.Scalar.BDecimal.newBuilder()
								.setScale(bigDecimal.scale())
								.setIntVal(OperandProto.Operand.Scalar.BInteger.newBuilder()
										.setValue(ByteString.copyFrom(bigDecimal.unscaledValue().toByteArray())))
				);
			} else if (value instanceof Boolean aBoolean) {
				scalarBuilder.setBoolean(aBoolean);
			} else if (value instanceof String aString) {
				scalarBuilder.setString(aString);
			} else if (value instanceof Integer anInteger) {
				scalarBuilder.setInteger(anInteger);
			} else if (value instanceof Long aLong) {
				scalarBuilder.setLong(aLong);
			} else if (value instanceof Float aFloat) {
				scalarBuilder.setFloat(aFloat);
			} else if (value instanceof Double aDouble) {
				scalarBuilder.setDouble(aDouble);
			} else if (value instanceof LocalDate date) {
				scalarBuilder.setDate(
						OperandProto.Operand.Scalar.Date.newBuilder()
								.setYear(date.getYear())
								.setMonth(date.getMonthValue())
								.setDay(date.getDayOfMonth())
				);
			} else if (value instanceof LocalTime time) {
				scalarBuilder.setTime(
						OperandProto.Operand.Scalar.Time.newBuilder()
								.setHour(time.getHour())
								.setMinute(time.getMinute())
								.setSecond(time.getSecond())
								.setNano(time.getNano())
				);
			} else if (value instanceof Instant instant) {
				scalarBuilder.setTimestamp(
						OperandProto.Operand.Scalar.Timestamp.newBuilder()
								.setSeconds(instant.getEpochSecond())
								.setNanos(instant.getNano())
				);
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
		} else if (operand instanceof OperandCast operandCast) {
			builder.setCast(
					OperandProto.Operand.Cast.newBuilder()
							.setValueOperand(convert(operandCast.getValueOperand()))
							.setType(operandCast.getType())
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
						case BIG_DECIMAL -> {
							OperandProto.Operand.Scalar.BDecimal bDecimal = operand.getScalar().getBigDecimal();
							BigDecimal bigDecimal = new BigDecimal(
									new BigInteger(bDecimal.getIntVal().getValue().toByteArray()),
									bDecimal.getScale()
							);
							yield Value.materializedValue(BigDecimal.class, bigDecimal);
						}
						case INTEGER -> Value.materializedValue(int.class, operand.getScalar().getInteger());
						case LONG -> Value.materializedValue(long.class, operand.getScalar().getLong());
						case FLOAT -> Value.materializedValue(float.class, operand.getScalar().getFloat());
						case DOUBLE -> Value.materializedValue(double.class, operand.getScalar().getDouble());
						case BOOLEAN -> Value.materializedValue(boolean.class, operand.getScalar().getBoolean());
						case STRING -> Value.materializedValue(String.class, operand.getScalar().getString());
						case DATE -> {
							OperandProto.Operand.Scalar.Date date = operand.getScalar().getDate();
							yield Value.materializedValue(LocalDate.class, LocalDate.of(
											date.getYear(),
											date.getMonth(),
											date.getDay()
									)
							);
						}
						case TIME -> {
							OperandProto.Operand.Scalar.Time time = operand.getScalar().getTime();
							yield Value.materializedValue(LocalTime.class, LocalTime.of(
											time.getHour(),
											time.getMinute(),
											time.getSecond(),
											time.getNano()
									)
							);
						}
						case TIMESTAMP -> {
							OperandProto.Operand.Scalar.Timestamp timestamp = operand.getScalar().getTimestamp();
							yield Value.materializedValue(Instant.class, Instant.ofEpochSecond(
									timestamp.getSeconds(),
									timestamp.getNanos()
							));
						}
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
			case CAST -> new OperandCast(
					convert(classLoader, operand.getCast().getValueOperand()),
					operand.getCast().getType()
			);
			case OPERAND_NOT_SET -> throw new CorruptedDataException("Operand not set");
		};
	}
}
