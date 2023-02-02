package io.activej.dataflow.calcite.where;

import io.activej.common.annotation.StaticFactories;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.impl.*;

import java.util.Collection;
import java.util.List;

@StaticFactories(WherePredicate.class)
public final class WherePredicates {
	public static WherePredicate and(WherePredicate... predicates) {
		return and(List.of(predicates));
	}

	public static WherePredicate and(List<WherePredicate> predicates) {
		return new And(predicates);
	}

	public static WherePredicate or(WherePredicate... predicates) {
		return or(List.of(predicates));
	}

	public static WherePredicate or(List<WherePredicate> predicates) {
		return new Or(predicates);
	}

	public static WherePredicate between(Operand<?> value, Operand<?> from, Operand<?> to) {
		return new Between(value, from, to);
	}

	public static WherePredicate eq(Operand<?> left, Operand<?> right) {
		return new Eq(left, right);
	}

	public static WherePredicate notEq(Operand<?> left, Operand<?> right) {
		return new NotEq(left, right);
	}

	public static WherePredicate ge(Operand<?> left, Operand<?> right) {
		return new Ge(left, right);
	}

	public static WherePredicate gt(Operand<?> left, Operand<?> right) {
		return new Gt(left, right);
	}

	public static WherePredicate le(Operand<?> left, Operand<?> right) {
		return new Le(left, right);
	}

	public static WherePredicate lt(Operand<?> left, Operand<?> right) {
		return new Lt(left, right);
	}

	public static WherePredicate in(Operand<?> value, Collection<Operand<?>> options) {
		return new In(value, options);
	}

	public static WherePredicate isNull(Operand<?> value) {
		return new IsNull(value);
	}

	public static WherePredicate isNotNull(Operand<?> value) {
		return new IsNotNull(value);
	}

	public static WherePredicate like(Operand<?> value, Operand<?> pattern) {
		return new Like(value, pattern);
	}
}
