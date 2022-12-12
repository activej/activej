package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.*;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

final class WherePredicateCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<AndPredicate> andPredicate(
			StreamCodec<WherePredicate> wherePredicateStreamCodec
	) {
		return StreamCodec.create(AndPredicate::new,
				AndPredicate::getPredicates, StreamCodecs.ofList(wherePredicateStreamCodec)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<OrPredicate> orPredicate(
			StreamCodec<WherePredicate> wherePredicateStreamCodec
	) {
		return StreamCodec.create(OrPredicate::new,
				OrPredicate::getPredicates, StreamCodecs.ofList(wherePredicateStreamCodec)
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<EqPredicate> eqPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(EqPredicate::new,
				EqPredicate::getLeft, operandStreamCodec,
				EqPredicate::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<NotEqPredicate> notEqPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(NotEqPredicate::new,
				NotEqPredicate::getLeft, operandStreamCodec,
				NotEqPredicate::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(4)
	StreamCodec<GePredicate> gePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(GePredicate::new,
				GePredicate::getLeft, operandStreamCodec,
				GePredicate::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<GtPredicate> gtPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(GtPredicate::new,
				GtPredicate::getLeft, operandStreamCodec,
				GtPredicate::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<LePredicate> lePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(LePredicate::new,
				LePredicate::getLeft, operandStreamCodec,
				LePredicate::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(7)
	StreamCodec<LtPredicate> ltPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(LtPredicate::new,
				LtPredicate::getLeft, operandStreamCodec,
				LtPredicate::getRight, operandStreamCodec
		);
	}

	@Provides
	@Subtype(8)
	StreamCodec<BetweenPredicate> betweenPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(BetweenPredicate::new,
				BetweenPredicate::getValue, operandStreamCodec,
				BetweenPredicate::getFrom, operandStreamCodec,
				BetweenPredicate::getTo, operandStreamCodec
		);
	}

	@Provides
	@Subtype(9)
	StreamCodec<LikePredicate> likePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(LikePredicate::new,
				LikePredicate::getValue, operandStreamCodec,
				LikePredicate::getPattern, operandStreamCodec
		);
	}

	@Provides
	@Subtype(10)
	StreamCodec<InPredicate> inPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(InPredicate::new,
				InPredicate::getValue, operandStreamCodec,
				InPredicate::getOptions, StreamCodecs.ofList(operandStreamCodec)
		);
	}

	@Provides
	@Subtype(11)
	StreamCodec<IsNullPredicate> isNullPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(IsNullPredicate::new,
				IsNullPredicate::getValue, operandStreamCodec
		);
	}

	@Provides
	@Subtype(12)
	StreamCodec<IsNotNullPredicate> isNotNullPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(IsNotNullPredicate::new,
				IsNotNullPredicate::getValue, operandStreamCodec
		);
	}
}
