package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.*;
import io.activej.dataflow.calcite.where.impl.*;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

public final class WherePredicateCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<And> andPredicate(
			StreamCodec<WherePredicate> wherePredicateStreamCodec
	) {
		return StreamCodec.create(And::new,
				and -> and.predicates, StreamCodecs.ofList(wherePredicateStreamCodec)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<Or> orPredicate(
			StreamCodec<WherePredicate> wherePredicateStreamCodec
	) {
		return StreamCodec.create(Or::new,
				or -> or.predicates, StreamCodecs.ofList(wherePredicateStreamCodec)
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<Eq> eqPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Eq::new,
				eq -> eq.left, operandStreamCodec,
				eq -> eq.right, operandStreamCodec
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<NotEq> notEqPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(NotEq::new,
				notEq -> notEq.left, operandStreamCodec,
				notEq -> notEq.right, operandStreamCodec
		);
	}

	@Provides
	@Subtype(4)
	StreamCodec<Ge> gePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Ge::new,
				ge -> ge.left, operandStreamCodec,
				ge -> ge.right, operandStreamCodec
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<Gt> gtPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Gt::new,
				gt -> gt.left, operandStreamCodec,
				gt -> gt.right, operandStreamCodec
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<Le> lePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Le::new,
				le -> le.left, operandStreamCodec,
				le -> le.right, operandStreamCodec
		);
	}

	@Provides
	@Subtype(7)
	StreamCodec<Lt> ltPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Lt::new,
				lt -> lt.left, operandStreamCodec,
				lt -> lt.right, operandStreamCodec
		);
	}

	@Provides
	@Subtype(8)
	StreamCodec<Between> betweenPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Between::new,
				between -> between.value, operandStreamCodec,
				between -> between.from, operandStreamCodec,
				between -> between.to, operandStreamCodec
		);
	}

	@Provides
	@Subtype(9)
	StreamCodec<Like> likePredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Like::new,
				like -> like.value, operandStreamCodec,
				like -> like.pattern, operandStreamCodec
		);
	}

	@Provides
	@Subtype(10)
	StreamCodec<In> inPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(In::new,
				in -> in.value, operandStreamCodec,
				in -> in.options, StreamCodecs.ofCollection(operandStreamCodec)
		);
	}

	@Provides
	@Subtype(11)
	StreamCodec<IsNull> isNullPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(IsNull::new,
				isNull -> isNull.value, operandStreamCodec
		);
	}

	@Provides
	@Subtype(12)
	StreamCodec<IsNotNull> isNotNullPredicate(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(IsNotNull::new,
				isNotNull -> isNotNull.value, operandStreamCodec
		);
	}
}
