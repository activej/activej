package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.where.*;
import io.activej.dataflow.proto.calcite.WherePredicateProto;
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

		return convert(predicate, classLoader);
	}

	public static WherePredicateProto.WherePredicate convert(WherePredicate predicate) {
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
							.setLeft(OperandConverter.convert(eqPredicate.getLeft()))
							.setRight(OperandConverter.convert(eqPredicate.getRight()))
			);
		} else if (predicate instanceof NotEqPredicate notEqPredicate) {
			builder.setNotEqPredicate(
					WherePredicateProto.WherePredicate.NotEqPredicate.newBuilder()
							.setLeft(OperandConverter.convert(notEqPredicate.getLeft()))
							.setRight(OperandConverter.convert(notEqPredicate.getRight()))
			);
		} else if (predicate instanceof GePredicate gePredicate) {
			builder.setGePredicate(
					WherePredicateProto.WherePredicate.GePredicate.newBuilder()
							.setLeft(OperandConverter.convert(gePredicate.getLeft()))
							.setRight(OperandConverter.convert(gePredicate.getRight()))
			);
		} else if (predicate instanceof GtPredicate gtPredicate) {
			builder.setGtPredicate(
					WherePredicateProto.WherePredicate.GtPredicate.newBuilder()
							.setLeft(OperandConverter.convert(gtPredicate.getLeft()))
							.setRight(OperandConverter.convert(gtPredicate.getRight()))
			);
		} else if (predicate instanceof LePredicate lePredicate) {
			builder.setLePredicate(
					WherePredicateProto.WherePredicate.LePredicate.newBuilder()
							.setLeft(OperandConverter.convert(lePredicate.getLeft()))
							.setRight(OperandConverter.convert(lePredicate.getRight()))
			);
		} else if (predicate instanceof LtPredicate ltPredicate) {
			builder.setLtPredicate(
					WherePredicateProto.WherePredicate.LtPredicate.newBuilder()
							.setLeft(OperandConverter.convert(ltPredicate.getLeft()))
							.setRight(OperandConverter.convert(ltPredicate.getRight()))
			);
		} else if (predicate instanceof BetweenPredicate betweenPredicate) {
			builder.setBetweenPredicate(
					WherePredicateProto.WherePredicate.BetweenPredicate.newBuilder()
							.setValue(OperandConverter.convert(betweenPredicate.getValue()))
							.setFrom(OperandConverter.convert(betweenPredicate.getFrom()))
							.setTo(OperandConverter.convert(betweenPredicate.getTo()))
			);
		} else if (predicate instanceof InPredicate inPredicate) {
			builder.setInPredicate(
					WherePredicateProto.WherePredicate.InPredicate.newBuilder()
							.setValue(OperandConverter.convert(inPredicate.getValue()))
							.addAllOptions(inPredicate.getOptions().stream()
									.map(OperandConverter::convert)
									.toList())
			);
		} else if (predicate instanceof LikePredicate likePredicate) {
			builder.setLikePredicate(
					WherePredicateProto.WherePredicate.LikePredicate.newBuilder()
							.setValue(OperandConverter.convert(likePredicate.getValue()))
							.setPattern(OperandConverter.convert(likePredicate.getPattern()))
			);
		} else if (predicate instanceof IsNullPredicate isNullPredicate) {
			builder.setIsNullPredicate(
					WherePredicateProto.WherePredicate.IsNullPredicate.newBuilder()
							.setValue(OperandConverter.convert(isNullPredicate.getValue()))
			);
		} else if (predicate instanceof IsNotNullPredicate isNotNullPredicate) {
			builder.setIsNotNullPredicate(
					WherePredicateProto.WherePredicate.IsNotNullPredicate.newBuilder()
							.setValue(OperandConverter.convert(isNotNullPredicate.getValue()))
			);
		} else {
			throw new IllegalArgumentException("Unknown predicate type: " + predicate.getClass());
		}

		return builder.build();
	}

	public static WherePredicate convert(WherePredicateProto.WherePredicate predicate, DefiningClassLoader classLoader) {
		return switch (predicate.getOperandCase()) {
			case AND_PREDICATE -> new AndPredicate(
					predicate.getAndPredicate().getPredicatesList().stream()
							.map(wherePredicate -> convert(wherePredicate, classLoader))
							.toList()
			);
			case OR_PREDICATE -> new OrPredicate(
					predicate.getOrPredicate().getPredicatesList().stream()
							.map(wherePredicate -> convert(wherePredicate, classLoader))
							.toList()
			);
			case EQ_PREDICATE -> new EqPredicate(
					OperandConverter.convert(classLoader, predicate.getEqPredicate().getLeft()),
					OperandConverter.convert(classLoader, predicate.getEqPredicate().getRight())
			);
			case NOT_EQ_PREDICATE -> new NotEqPredicate(
					OperandConverter.convert(classLoader, predicate.getNotEqPredicate().getLeft()),
					OperandConverter.convert(classLoader, predicate.getNotEqPredicate().getRight())
			);
			case GE_PREDICATE -> new GePredicate(
					OperandConverter.convert(classLoader, predicate.getGePredicate().getLeft()),
					OperandConverter.convert(classLoader, predicate.getGePredicate().getRight())
			);
			case GT_PREDICATE -> new GtPredicate(
					OperandConverter.convert(classLoader, predicate.getGtPredicate().getLeft()),
					OperandConverter.convert(classLoader, predicate.getGtPredicate().getRight())
			);
			case LE_PREDICATE -> new LePredicate(
					OperandConverter.convert(classLoader, predicate.getLePredicate().getLeft()),
					OperandConverter.convert(classLoader, predicate.getLePredicate().getRight())
			);
			case LT_PREDICATE -> new LtPredicate(
					OperandConverter.convert(classLoader, predicate.getLtPredicate().getLeft()),
					OperandConverter.convert(classLoader, predicate.getLtPredicate().getRight())
			);
			case BETWEEN_PREDICATE -> new BetweenPredicate(
					OperandConverter.convert(classLoader, predicate.getBetweenPredicate().getValue()),
					OperandConverter.convert(classLoader, predicate.getBetweenPredicate().getFrom()),
					OperandConverter.convert(classLoader, predicate.getBetweenPredicate().getTo())
			);
			case IN_PREDICATE -> new InPredicate(
					OperandConverter.convert(classLoader, predicate.getInPredicate().getValue()),
					predicate.getInPredicate().getOptionsList().stream()
							.map(operand -> OperandConverter.convert(classLoader, operand))
							.collect(Collectors.toList())
			);
			case LIKE_PREDICATE -> new LikePredicate(
					OperandConverter.convert(classLoader, predicate.getLikePredicate().getValue()),
					OperandConverter.convert(classLoader, predicate.getLikePredicate().getPattern())
			);
			case IS_NULL_PREDICATE ->
					new IsNullPredicate(OperandConverter.convert(classLoader, predicate.getIsNullPredicate().getValue()));
			case IS_NOT_NULL_PREDICATE ->
					new IsNotNullPredicate(OperandConverter.convert(classLoader, predicate.getIsNotNullPredicate().getValue()));
			case OPERAND_NOT_SET -> throw new CorruptedDataException("Predicate was not set");
		};
	}

}
