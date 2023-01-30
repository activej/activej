package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.*;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

public final class WherePredicateCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<WherePredicate_And> andPredicate(
			StreamCodec<WherePredicate> wherePredicateStreamCodec
	) {
		return StreamCodec.create(WherePredicate_And::new,
				WherePredicate_And::getPredicates, StreamCodecs.ofList(wherePredicateStreamCodec)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<WherePredicate_Or> orPredicate(
			StreamCodec<WherePredicate> wherePredicateStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Or::new,
				WherePredicate_Or::getPredicates, StreamCodecs.ofList(wherePredicateStreamCodec)
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<WherePredicate_Eq> eqPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Eq::new,
				WherePredicate_Eq::getLeft, operandStreamCodec,
				WherePredicate_Eq::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<WherePredicate_NotEq> notEqPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_NotEq::new,
				WherePredicate_NotEq::getLeft, operandStreamCodec,
				WherePredicate_NotEq::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(4)
	StreamCodec<WherePredicate_Ge> gePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Ge::new,
				WherePredicate_Ge::getLeft, operandStreamCodec,
				WherePredicate_Ge::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<WherePredicate_Gt> gtPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Gt::new,
				WherePredicate_Gt::getLeft, operandStreamCodec,
				WherePredicate_Gt::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<WherePredicate_Le> lePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Le::new,
				WherePredicate_Le::getLeft, operandStreamCodec,
				WherePredicate_Le::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(7)
	StreamCodec<WherePredicate_Lt> ltPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Lt::new,
				WherePredicate_Lt::getLeft, operandStreamCodec,
				WherePredicate_Lt::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(8)
	StreamCodec<WherePredicate_Between> betweenPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Between::new,
				WherePredicate_Between::getValue, operandStreamCodec,
				WherePredicate_Between::getFrom, operandStreamCodec,
				WherePredicate_Between::getTo, operandStreamCodec
		);
	}

	@Provides
	@Subtype(9)
	StreamCodec<WherePredicate_Like> likePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_Like::new,
				WherePredicate_Like::getValue, operandStreamCodec,
				WherePredicate_Like::getPattern, operandStreamCodec
		);
	}

	@Provides
	@Subtype(10)
	StreamCodec<WherePredicate_In> inPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_In::new,
				WherePredicate_In::getValue, operandStreamCodec,
				WherePredicate_In::getOptions, StreamCodecs.ofList(operandStreamCodec)
		);
	}

	@Provides
	@Subtype(11)
	StreamCodec<WherePredicate_IsNull> isNullPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_IsNull::new,
				WherePredicate_IsNull::getValue, operandStreamCodec
		);
	}

	@Provides
	@Subtype(12)
	StreamCodec<WherePredicate_IsNotNull> isNotNullPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(WherePredicate_IsNotNull::new,
				WherePredicate_IsNotNull::getValue, operandStreamCodec
		);
	}
}
