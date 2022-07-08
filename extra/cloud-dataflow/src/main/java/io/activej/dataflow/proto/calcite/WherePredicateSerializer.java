package io.activej.dataflow.proto.calcite;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.where.*;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import static io.activej.serializer.BinarySerializers.BYTES_SERIALIZER;


public final class WherePredicateSerializer implements BinarySerializer<WherePredicate> {
	private final DefiningClassLoader classLoader;

	public WherePredicateSerializer(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public WherePredicateSerializer() {
		this(DefiningClassLoader.create());
	}

	@Override
	public void encode(BinaryOutput out, WherePredicate predicate) {
		BYTES_SERIALIZER.encode(out, convert(predicate).toByteArray());
	}

	@Override
	public WherePredicate decode(BinaryInput in) throws CorruptedDataException {
		byte[] serialized = BYTES_SERIALIZER.decode(in);

		WherePredicateProto.WherePredicate predicate;
		try {
			predicate = WherePredicateProto.WherePredicate.parseFrom(serialized);
		} catch (InvalidProtocolBufferException e) {
			throw new CorruptedDataException(e.getMessage());
		}

		return convert(predicate);
	}

	private static WherePredicateProto.WherePredicate convert(WherePredicate predicate) {
		WherePredicateProto.WherePredicate.Builder builder = WherePredicateProto.WherePredicate.newBuilder();

		if (predicate instanceof AndPredicate andPredicate) {
			builder.setAndPredicate(
					WherePredicateProto.WherePredicate.AndPredicate.newBuilder()
							.addAllPredicates(andPredicate.getPredicates().stream()
									.map(WherePredicateSerializer::convert)
									.toList())
			);
		} else if (predicate instanceof OrPredicate orPredicate) {
			builder.setOrPredicate(
					WherePredicateProto.WherePredicate.OrPredicate.newBuilder()
							.addAllPredicates(orPredicate.predicates().stream()
									.map(WherePredicateSerializer::convert)
									.toList())
			);
		} else if (predicate instanceof EqPredicate eqPredicate) {
			builder.setEqPredicate(
					WherePredicateProto.WherePredicate.EqPredicate.newBuilder()
							.setLeft(convert(eqPredicate.getLeft()))
							.setRight(convert(eqPredicate.getRight()))
			);
		} else if (predicate instanceof NotEqPredicate notEqPredicate) {
			builder.setNotEqPredicate(
					WherePredicateProto.WherePredicate.NotEqPredicate.newBuilder()
							.setLeft(convert(notEqPredicate.getLeft()))
							.setRight(convert(notEqPredicate.getRight()))
			);
		} else if (predicate instanceof GePredicate gePredicate) {
			builder.setGePredicate(
					WherePredicateProto.WherePredicate.GePredicate.newBuilder()
							.setLeft(convert(gePredicate.getLeft()))
							.setRight(convert(gePredicate.getRight()))
			);
		} else if (predicate instanceof GtPredicate gtPredicate) {
			builder.setGtPredicate(
					WherePredicateProto.WherePredicate.GtPredicate.newBuilder()
							.setLeft(convert(gtPredicate.getLeft()))
							.setRight(convert(gtPredicate.getRight()))
			);
		} else if (predicate instanceof LePredicate lePredicate) {
			builder.setLePredicate(
					WherePredicateProto.WherePredicate.LePredicate.newBuilder()
							.setLeft(convert(lePredicate.getLeft()))
							.setRight(convert(lePredicate.getRight()))
			);
		} else if (predicate instanceof LtPredicate ltPredicate) {
			builder.setLtPredicate(
					WherePredicateProto.WherePredicate.LtPredicate.newBuilder()
							.setLeft(convert(ltPredicate.getLeft()))
							.setRight(convert(ltPredicate.getRight()))
			);
		} else if (predicate instanceof BetweenPredicate betweenPredicate) {
			builder.setBetweenPredicate(
					WherePredicateProto.WherePredicate.BetweenPredicate.newBuilder()
							.setValue(convert(betweenPredicate.getValue()))
							.setFrom(convert(betweenPredicate.getFrom()))
							.setTo(convert(betweenPredicate.getTo()))
			);
		} else if (predicate instanceof InPredicate inPredicate) {
			builder.setInPredicate(
					WherePredicateProto.WherePredicate.InPredicate.newBuilder()
							.setValue(convert(inPredicate.getValue()))
							.addAllOptions(inPredicate.getOptions().stream()
									.map(WherePredicateSerializer::convert)
									.toList())
			);
		} else if (predicate instanceof LikePredicate likePredicate) {
			builder.setLikePredicate(
					WherePredicateProto.WherePredicate.LikePredicate.newBuilder()
							.setValue(convert(likePredicate.getValue()))
							.setPattern(convert(likePredicate.getPattern()))
			);
		} else {
			throw new IllegalArgumentException("Unknown predicate type: " + predicate.getClass());
		}

		return builder.build();
	}

	private WherePredicate convert(WherePredicateProto.WherePredicate predicate) {
		return switch (predicate.getOperandCase()) {
			case AND_PREDICATE -> new AndPredicate(
					predicate.getAndPredicate().getPredicatesList().stream()
							.map(this::convert)
							.toList()
			);
			case OR_PREDICATE -> new OrPredicate(
					predicate.getOrPredicate().getPredicatesList().stream()
							.map(this::convert)
							.toList()
			);
			case EQ_PREDICATE -> new EqPredicate(
					convert(predicate.getEqPredicate().getLeft()),
					convert(predicate.getEqPredicate().getRight())
			);
			case NOT_EQ_PREDICATE -> new NotEqPredicate(
					convert(predicate.getNotEqPredicate().getLeft()),
					convert(predicate.getNotEqPredicate().getRight())
			);
			case GE_PREDICATE -> new GePredicate(
					convert(predicate.getGePredicate().getLeft()),
					convert(predicate.getGePredicate().getRight())
			);
			case GT_PREDICATE -> new GtPredicate(
					convert(predicate.getGtPredicate().getLeft()),
					convert(predicate.getGtPredicate().getRight())
			);
			case LE_PREDICATE -> new LePredicate(
					convert(predicate.getLePredicate().getLeft()),
					convert(predicate.getLePredicate().getRight())
			);
			case LT_PREDICATE -> new LtPredicate(
					convert(predicate.getLtPredicate().getLeft()),
					convert(predicate.getLtPredicate().getRight())
			);
			case BETWEEN_PREDICATE -> new BetweenPredicate(
					convert(predicate.getBetweenPredicate().getValue()),
					convert(predicate.getBetweenPredicate().getFrom()),
					convert(predicate.getBetweenPredicate().getTo())
			);
			case IN_PREDICATE -> new InPredicate(
					convert(predicate.getInPredicate().getValue()),
					predicate.getInPredicate().getOptionsList().stream()
							.map(this::convert)
							.toList()
			);
			case LIKE_PREDICATE -> new LikePredicate(
					convert(predicate.getLikePredicate().getValue()),
					convert(predicate.getLikePredicate().getPattern())
			);
			case OPERAND_NOT_SET -> throw new CorruptedDataException("Predicate was not set");
		};
	}

	private static OperandProto.Operand convert(Operand operand) {
		OperandProto.Operand.Builder builder = OperandProto.Operand.newBuilder();

		if (operand instanceof OperandRecordField operandRecordField) {
			builder.setRecordField(
					OperandProto.Operand.RecordField.newBuilder()
							.setIndex(operandRecordField.getIndex())
			);
		} else if (operand instanceof OperandScalar operandScalar) {
			OperandProto.Operand.Scalar.Builder scalarBuilder = OperandProto.Operand.Scalar.newBuilder();

			Object value = operandScalar.getValue();
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
							.setFieldNameOperand(convert(operandFieldAccess.getFieldNameOperand()))
			);
		} else {
			throw new IllegalArgumentException("Unknown operand type: " + operand.getClass());
		}

		return builder.build();
	}

	private Operand convert(OperandProto.Operand operand) {
		return switch (operand.getOperandCase()) {
			case RECORD_FIELD -> new OperandRecordField(operand.getRecordField().getIndex());
			case SCALAR -> new OperandScalar(
					switch (operand.getScalar().getValueCase()) {
						case NULL -> null;
						case INTEGER -> operand.getScalar().getInteger();
						case LONG -> operand.getScalar().getLong();
						case FLOAT -> operand.getScalar().getFloat();
						case DOUBLE -> operand.getScalar().getDouble();
						case BOOLEAN -> operand.getScalar().getBoolean();
						case STRING -> operand.getScalar().getString();
						case VALUE_NOT_SET -> throw new CorruptedDataException("Scalar value not set");
					}
			);
			case MAP_GET -> new OperandMapGet<>(
					convert(operand.getMapGet().getMapOperand()),
					convert(operand.getMapGet().getKeyOperand())
			);
			case LIST_GET -> new OperandListGet(
					convert(operand.getListGet().getListOperand()),
					convert(operand.getListGet().getIndexOperand())
			);
			case FIELD_GET -> new OperandFieldAccess(
					convert(operand.getFieldGet().getObjectOperand()),
					convert(operand.getFieldGet().getFieldNameOperand()),
					classLoader
			);
			case OPERAND_NOT_SET -> throw new CorruptedDataException("Operand not set");
		};
	}

}
