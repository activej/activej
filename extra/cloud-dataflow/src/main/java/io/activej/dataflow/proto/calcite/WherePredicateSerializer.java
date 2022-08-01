package io.activej.dataflow.proto.calcite;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.where.*;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.util.stream.Collectors;

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
							.addAllPredicates(orPredicate.getPredicates().stream()
									.map(WherePredicateSerializer::convert)
									.toList())
			);
		} else if (predicate instanceof EqPredicate eqPredicate) {
			builder.setEqPredicate(
					WherePredicateProto.WherePredicate.EqPredicate.newBuilder()
							.setLeft(OperandConverters.convert(eqPredicate.getLeft()))
							.setRight(OperandConverters.convert(eqPredicate.getRight()))
			);
		} else if (predicate instanceof NotEqPredicate notEqPredicate) {
			builder.setNotEqPredicate(
					WherePredicateProto.WherePredicate.NotEqPredicate.newBuilder()
							.setLeft(OperandConverters.convert(notEqPredicate.getLeft()))
							.setRight(OperandConverters.convert(notEqPredicate.getRight()))
			);
		} else if (predicate instanceof GePredicate gePredicate) {
			builder.setGePredicate(
					WherePredicateProto.WherePredicate.GePredicate.newBuilder()
							.setLeft(OperandConverters.convert(gePredicate.getLeft()))
							.setRight(OperandConverters.convert(gePredicate.getRight()))
			);
		} else if (predicate instanceof GtPredicate gtPredicate) {
			builder.setGtPredicate(
					WherePredicateProto.WherePredicate.GtPredicate.newBuilder()
							.setLeft(OperandConverters.convert(gtPredicate.getLeft()))
							.setRight(OperandConverters.convert(gtPredicate.getRight()))
			);
		} else if (predicate instanceof LePredicate lePredicate) {
			builder.setLePredicate(
					WherePredicateProto.WherePredicate.LePredicate.newBuilder()
							.setLeft(OperandConverters.convert(lePredicate.getLeft()))
							.setRight(OperandConverters.convert(lePredicate.getRight()))
			);
		} else if (predicate instanceof LtPredicate ltPredicate) {
			builder.setLtPredicate(
					WherePredicateProto.WherePredicate.LtPredicate.newBuilder()
							.setLeft(OperandConverters.convert(ltPredicate.getLeft()))
							.setRight(OperandConverters.convert(ltPredicate.getRight()))
			);
		} else if (predicate instanceof BetweenPredicate betweenPredicate) {
			builder.setBetweenPredicate(
					WherePredicateProto.WherePredicate.BetweenPredicate.newBuilder()
							.setValue(OperandConverters.convert(betweenPredicate.getValue()))
							.setFrom(OperandConverters.convert(betweenPredicate.getFrom()))
							.setTo(OperandConverters.convert(betweenPredicate.getTo()))
			);
		} else if (predicate instanceof InPredicate inPredicate) {
			builder.setInPredicate(
					WherePredicateProto.WherePredicate.InPredicate.newBuilder()
							.setValue(OperandConverters.convert(inPredicate.getValue()))
							.addAllOptions(inPredicate.getOptions().stream()
									.map(OperandConverters::convert)
									.toList())
			);
		} else if (predicate instanceof LikePredicate likePredicate) {
			builder.setLikePredicate(
					WherePredicateProto.WherePredicate.LikePredicate.newBuilder()
							.setValue(OperandConverters.convert(likePredicate.getValue()))
							.setPattern(OperandConverters.convert(likePredicate.getPattern()))
			);
		} else if (predicate instanceof IsNullPredicate isNullPredicate) {
			builder.setIsNullPredicate(
					WherePredicateProto.WherePredicate.IsNullPredicate.newBuilder()
							.setValue(OperandConverters.convert(isNullPredicate.getValue()))
			);
		} else if (predicate instanceof IsNotNullPredicate isNotNullPredicate) {
			builder.setIsNotNullPredicate(
					WherePredicateProto.WherePredicate.IsNotNullPredicate.newBuilder()
							.setValue(OperandConverters.convert(isNotNullPredicate.getValue()))
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
					OperandConverters.convert(classLoader, predicate.getEqPredicate().getLeft()),
					OperandConverters.convert(classLoader, predicate.getEqPredicate().getRight())
			);
			case NOT_EQ_PREDICATE -> new NotEqPredicate(
					OperandConverters.convert(classLoader, predicate.getNotEqPredicate().getLeft()),
					OperandConverters.convert(classLoader, predicate.getNotEqPredicate().getRight())
			);
			case GE_PREDICATE -> new GePredicate(
					OperandConverters.convert(classLoader, predicate.getGePredicate().getLeft()),
					OperandConverters.convert(classLoader, predicate.getGePredicate().getRight())
			);
			case GT_PREDICATE -> new GtPredicate(
					OperandConverters.convert(classLoader, predicate.getGtPredicate().getLeft()),
					OperandConverters.convert(classLoader, predicate.getGtPredicate().getRight())
			);
			case LE_PREDICATE -> new LePredicate(
					OperandConverters.convert(classLoader, predicate.getLePredicate().getLeft()),
					OperandConverters.convert(classLoader, predicate.getLePredicate().getRight())
			);
			case LT_PREDICATE -> new LtPredicate(
					OperandConverters.convert(classLoader, predicate.getLtPredicate().getLeft()),
					OperandConverters.convert(classLoader, predicate.getLtPredicate().getRight())
			);
			case BETWEEN_PREDICATE -> new BetweenPredicate(
					OperandConverters.convert(classLoader, predicate.getBetweenPredicate().getValue()),
					OperandConverters.convert(classLoader, predicate.getBetweenPredicate().getFrom()),
					OperandConverters.convert(classLoader, predicate.getBetweenPredicate().getTo())
			);
			case IN_PREDICATE -> new InPredicate(
					OperandConverters.convert(classLoader, predicate.getInPredicate().getValue()),
					predicate.getInPredicate().getOptionsList().stream()
							.map(operand -> OperandConverters.convert(classLoader, operand))
							.collect(Collectors.toList())
			);
			case LIKE_PREDICATE -> new LikePredicate(
					OperandConverters.convert(classLoader, predicate.getLikePredicate().getValue()),
					OperandConverters.convert(classLoader, predicate.getLikePredicate().getPattern())
			);
			case IS_NULL_PREDICATE -> new IsNullPredicate(OperandConverters.convert(classLoader, predicate.getIsNullPredicate().getValue()));
			case IS_NOT_NULL_PREDICATE -> new IsNotNullPredicate(OperandConverters.convert(classLoader, predicate.getIsNotNullPredicate().getValue()));
			case OPERAND_NOT_SET -> throw new CorruptedDataException("Predicate was not set");
		};
	}

}
