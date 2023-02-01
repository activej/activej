/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.aggregation.predicate;

import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.regex.Pattern;

import static io.activej.codegen.expression.Expressions.value;
import static io.activej.common.Checks.checkState;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationPredicates {

	public record PredicateSimplifierKey<L extends PredicateDef, R extends PredicateDef>(
			Class<L> leftType, Class<R> rightType) {
	}

	@FunctionalInterface
	public interface PredicateSimplifier<L extends PredicateDef, R extends PredicateDef> {
		PredicateDef simplifyAnd(L left, R right);
	}

	static final Map<PredicateSimplifierKey<?, ?>, PredicateSimplifier<?, ?>> simplifiers = new HashMap<>();

	public static <L extends PredicateDef, R extends PredicateDef> void register(Class<L> leftType, Class<R> rightType, PredicateSimplifier<L, R> operation) {
		PredicateSimplifierKey<L, R> keyLeftRight = new PredicateSimplifierKey<>(leftType, rightType);
		checkState(!simplifiers.containsKey(keyLeftRight), "Key '%s has already been registered", keyLeftRight);
		simplifiers.put(keyLeftRight, operation);
		if (!rightType.equals(leftType)) {
			PredicateSimplifierKey<R, L> keyRightLeft = new PredicateSimplifierKey<>(rightType, leftType);
			checkState(!simplifiers.containsKey(keyRightLeft), "Key '%s has already been registered", keyRightLeft);
			simplifiers.put(keyRightLeft, (PredicateSimplifier<R, L>) (right, left) -> operation.simplifyAnd(left, right));
		}
	}

	static {
		PredicateSimplifier simplifierAlwaysFalse = (PredicateSimplifier<PredicateDef_AlwaysFalse, PredicateDef>) (left, right) -> left;
		register(PredicateDef_AlwaysFalse.class, PredicateDef_AlwaysFalse.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_AlwaysTrue.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Not.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Eq.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_NotEq.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Le.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Ge.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Lt.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Gt.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Has.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Between.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_RegExp.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_And.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_Or.class, simplifierAlwaysFalse);
		register(PredicateDef_AlwaysFalse.class, PredicateDef_In.class, simplifierAlwaysFalse);

		PredicateSimplifier simplifierAlwaysTrue = (PredicateSimplifier<PredicateDef_AlwaysTrue, PredicateDef>) (left, right) -> right;
		register(PredicateDef_AlwaysTrue.class, PredicateDef_AlwaysTrue.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Not.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Eq.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_NotEq.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Le.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Ge.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Lt.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Gt.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Has.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Between.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_RegExp.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_And.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_Or.class, simplifierAlwaysTrue);
		register(PredicateDef_AlwaysTrue.class, PredicateDef_In.class, simplifierAlwaysTrue);

		PredicateSimplifier simplifierNot = (PredicateSimplifier<PredicateDef_Not, PredicateDef>) (left, right) -> {
			if (left.getPredicate().equals(right))
				return alwaysFalse();
			return null;
		};
		register(PredicateDef_Not.class, PredicateDef_Not.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_Has.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_Between.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_RegExp.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_And.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_Or.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_Ge.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_Le.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_Gt.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_Lt.class, simplifierNot);
		register(PredicateDef_Not.class, PredicateDef_In.class, simplifierNot);

		register(PredicateDef_Has.class, PredicateDef_Has.class, (left, right) -> left.getKey().equals(right.getKey()) ? left : null);
		PredicateSimplifier simplifierHas = (PredicateSimplifier<PredicateDef_Has, PredicateDef>) (left, right) -> right.getDimensions().contains(left.getKey()) ? right : null;
		register(PredicateDef_Has.class, PredicateDef_Eq.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_NotEq.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_Le.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_Ge.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_Lt.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_Gt.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_Between.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_And.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_Or.class, simplifierHas);
		register(PredicateDef_Has.class, PredicateDef_In.class, simplifierHas);

		register(PredicateDef_Eq.class, PredicateDef_Eq.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_NotEq.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (!left.getValue().equals(right.getValue()))
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Le.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) >= 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Ge.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) <= 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Lt.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) > 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) < 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Between.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getFrom().compareTo(left.getValue()) <= 0 && right.getTo().compareTo(left.getValue()) >= 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_RegExp.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue() instanceof CharSequence sequence &&
					right.getRegexpPattern().matcher(sequence).matches())
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValues().contains(left.getValue()))
				return left;
			return alwaysFalse();
		});

		register(PredicateDef_NotEq.class, PredicateDef_NotEq.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().equals(right.getValue()))
				return left;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Le.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) < 0)
				return right;
			if (right.getValue().compareTo(left.getValue()) == 0)
				return lt(left.getKey(), right.getValue());
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Ge.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) > 0)
				return right;
			if (right.getValue().compareTo(left.getValue()) == 0)
				return gt(left.getKey(), right.getValue());
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Lt.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) <= 0)
				return right;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) >= 0)
				return right;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getFrom().compareTo(left.getValue()) > 0 && right.getTo().compareTo(left.getValue()) > 0)
				return right;
			if (right.getFrom().compareTo(left.getValue()) < 0 && right.getTo().compareTo(left.getValue()) < 0)
				return right;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_RegExp.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue() instanceof CharSequence sequence &&
					right.getRegexpPattern().matcher(sequence).matches())
				return null;
			return right;
		});
		register(PredicateDef_NotEq.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (right.getValues().contains(left.getValue()))
				return alwaysFalse();
			return right;
		});

		register(PredicateDef_Le.class, PredicateDef_Le.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) <= 0)
				return right;
			return left;
		});
		register(PredicateDef_Le.class, PredicateDef_Ge.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().compareTo(right.getValue()) < 0)
				return alwaysFalse();
			if (left.getValue().compareTo(right.getValue()) > 0)
				return between(right.getKey(), right.getValue(), left.getValue());
			if (left.getValue().compareTo(right.getValue()) == 0)
				return eq(left.getKey(), left.getValue());
			return null;
		});
		register(PredicateDef_Le.class, PredicateDef_Lt.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) <= 0)
				return right;
			return left;
		});
		register(PredicateDef_Le.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().compareTo(right.getValue()) <= 0)
				return alwaysFalse();
			return null;
		});
		register(PredicateDef_Le.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getFrom().compareTo(left.getValue()) > 0)
				return alwaysFalse();
			if (right.getFrom().compareTo(left.getValue()) == 0)
				return eq(left.getKey(), right.getFrom());
			if (right.getTo().compareTo(left.getValue()) <= 0)
				return right;
			return between(right.getKey(), right.getFrom(), left.getValue()).simplify();
		});
		register(PredicateDef_Le.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().compareTo(right.getValues().last()) >= 0)
				return right;
			if (left.getValue().compareTo(right.getValues().first()) < 0)
				return alwaysFalse();
			SortedSet subset = new TreeSet(right.getValues().headSet(left.getValue()));
			if (right.getValues().contains(left.getValue())) subset.add(left.getValue());
			return in(left.getKey(), subset);
		});

		register(PredicateDef_Ge.class, PredicateDef_Ge.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) >= 0)
				return right;
			return left;
		});
		register(PredicateDef_Ge.class, PredicateDef_Lt.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) <= 0)
				return alwaysFalse();
			return null;
		});
		register(PredicateDef_Ge.class, PredicateDef_Gt.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) >= 0)
				return gt(right.getKey(), right.getValue());
			return left;
		});
		register(PredicateDef_Ge.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getTo().compareTo(left.getValue()) < 0)
				return alwaysFalse();
			if (right.getTo().compareTo(left.getValue()) == 0)
				return eq(right.getKey(), right.getTo());
			if (right.getFrom().compareTo(left.getValue()) >= 0)
				return right;
			return between(right.getKey(), left.getValue(), right.getTo()).simplify();
		});
		register(PredicateDef_Ge.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().compareTo(right.getValues().first()) <= 0)
				return right;
			if (left.getValue().compareTo(right.getValues().last()) > 0)
				return alwaysFalse();
			return in(left.getKey(), new TreeSet(right.getValues().tailSet(left.getValue())));
		});

		register(PredicateDef_Lt.class, PredicateDef_Lt.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) >= 0)
				return left;
			return right;
		});
		register(PredicateDef_Lt.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().compareTo(right.getValue()) <= 0)
				return alwaysFalse();
			return null;
		});
		register(PredicateDef_Lt.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getFrom().compareTo(left.getValue()) >= 0)
				return alwaysFalse();
			if (right.getTo().compareTo(left.getValue()) < 0)
				return right;
			return null;
		});
		register(PredicateDef_Lt.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().compareTo(right.getValues().last()) > 0)
				return right;
			if (left.getValue().compareTo(right.getValues().first()) < 0)
				return alwaysFalse();
			return in(left.getKey(), new TreeSet(right.getValues().subSet(right.getValues().first(), left.getValue())));
		});

		register(PredicateDef_Gt.class, PredicateDef_Gt.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getValue().compareTo(left.getValue()) >= 0)
				return right;
			return left;
		});
		register(PredicateDef_Gt.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.getKey().equals(left.getKey()))
				return null;
			if (right.getTo().compareTo(left.getValue()) <= 0)
				return alwaysFalse();
			if (right.getFrom().compareTo(left.getValue()) > 0)
				return right;
			return null;
		});
		register(PredicateDef_Gt.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValue().compareTo(right.getValues().first()) < 0)
				return right;
			if (left.getValue().compareTo(right.getValues().last()) >= 0)
				return alwaysFalse();
			SortedSet subset = right.getValues().tailSet(left.getValue());
			subset.remove(left.getValue());
			return in(right.getKey(), subset);
		});

		register(PredicateDef_Between.class, PredicateDef_Between.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			Comparable from = left.getFrom().compareTo(right.getFrom()) >= 0 ? left.getFrom() : right.getFrom();
			Comparable to = left.getTo().compareTo(right.getTo()) <= 0 ? left.getTo() : right.getTo();
			return between(left.getKey(), from, to).simplify();
		});
		register(PredicateDef_Between.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getFrom().compareTo(right.getValues().first()) > 0 && left.getTo().compareTo(right.getValues().last()) > 0)
				return left;
			return null;
		});

		register(PredicateDef_In.class, PredicateDef_In.class, (left, right) -> {
			if (!left.getKey().equals(right.getKey()))
				return null;
			if (left.getValues().equals(right.getValues()))
				return left.getValues().size() == 1 ? eq(left.getKey(), left.getValues().first()) : left;
			SortedSet values = left.getValues();
			values.retainAll(right.getValues());
			if (values.size() == 1)
				return eq(left.getKey(), left.getValues().first());
			if (!left.getValues().isEmpty())
				return in(left.getKey(), left.getValues());
			return alwaysFalse();
		});
	}

	public static PredicateDef alwaysTrue() {
		return PredicateDef_AlwaysTrue.INSTANCE;
	}

	public static PredicateDef alwaysFalse() {
		return PredicateDef_AlwaysFalse.INSTANCE;
	}

	public static PredicateDef not(PredicateDef predicate) {
		return new PredicateDef_Not(predicate);
	}

	public static PredicateDef and(List<PredicateDef> predicates) {
		return new PredicateDef_And(predicates);
	}

	public static PredicateDef and(PredicateDef... predicates) {
		return and(List.of(predicates));
	}

	public static PredicateDef or(List<PredicateDef> predicates) {
		return new PredicateDef_Or(predicates);
	}

	public static PredicateDef or(PredicateDef... predicates) {
		return or(List.of(predicates));
	}

	public static PredicateDef eq(String key, @Nullable Object value) {
		return new PredicateDef_Eq(key, value);
	}

	public static PredicateDef notEq(String key, Object value) {
		return new PredicateDef_NotEq(key, value);
	}

	public static PredicateDef ge(String key, Comparable value) {
		return new PredicateDef_Ge(key, value);
	}

	public static PredicateDef le(String key, Comparable value) {
		return new PredicateDef_Le(key, value);
	}

	public static PredicateDef gt(String key, Comparable value) {
		return new PredicateDef_Gt(key, value);
	}

	public static PredicateDef lt(String key, Comparable value) {
		return new PredicateDef_Lt(key, value);
	}

	public static PredicateDef has(String key) {
		return new PredicateDef_Has(key);
	}

	@SuppressWarnings("unchecked")
	public static PredicateDef in(String key, Collection values) {
		return values.size() == 1 ? new PredicateDef_Eq(key, values.toArray()[0]) : new PredicateDef_In(key, new TreeSet(values));
	}

	@SuppressWarnings("unchecked")
	public static PredicateDef in(String key, Comparable... values) {
		return values.length == 1 ? new PredicateDef_Eq(key, values[0]) : new PredicateDef_In(key, new TreeSet(List.of(values)));
	}

	public static PredicateDef regexp(String key, @Language("RegExp") String pattern) {
		return new PredicateDef_RegExp(key, Pattern.compile(pattern));
	}

	public static PredicateDef regexp(String key, Pattern pattern) {
		return new PredicateDef_RegExp(key, pattern);
	}

	public static <C extends Comparable<C>> PredicateDef between(String key, Comparable from, Comparable to) {
		return new PredicateDef_Between(key, from, to);
	}

	public static final class RangeScan {
		private final PrimaryKey from;
		private final PrimaryKey to;

		private RangeScan(PrimaryKey from, PrimaryKey to) {
			this.from = from;
			this.to = to;
		}

		public static RangeScan noScan() {
			return new RangeScan(null, null);
		}

		public static RangeScan fullScan() {
			return new RangeScan(PrimaryKey.ofArray(), PrimaryKey.ofArray());
		}

		public static RangeScan rangeScan(PrimaryKey from, PrimaryKey to) {
			return new RangeScan(from, to);
		}

		@SuppressWarnings("BooleanMethodIsAlwaysInverted")
		public boolean isNoScan() {
			return from == null;
		}

		public boolean isFullScan() {
			return from.size() == 0;
		}

		public boolean isRangeScan() {
			return !isNoScan() && !isFullScan();
		}

		public PrimaryKey getFrom() {
			checkState(!isNoScan(), "Cannot return 'from' in 'No Scan' mode");
			return from;
		}

		public PrimaryKey getTo() {
			checkState(!isNoScan(), "Cannot return 'to' in 'No Scan' mode");
			return to;
		}
	}

	static Expression isNotNull(Expression field, FieldType fieldType) {
		return fieldType != null && fieldType.getInternalDataType().isPrimitive() ? value(true) : Expressions.isNotNull(field);
	}

	static Expression isNull(Expression field, FieldType fieldType) {
		return fieldType != null && fieldType.getInternalDataType().isPrimitive() ? value(false) : Expressions.isNull(field);
	}

	@SuppressWarnings("unchecked")
	static Object toInternalValue(Map<String, FieldType> fields, String key, Object value) {
		return fields.containsKey(key) ? fields.get(key).toInternalValue(value) : value;
	}

	public static RangeScan toRangeScan(PredicateDef predicate, List<String> primaryKey, Map<String, FieldType> fields) {
		predicate = predicate.simplify();
		if (predicate == alwaysFalse())
			return RangeScan.noScan();
		List<PredicateDef> conjunctions = new ArrayList<>();
		if (predicate instanceof PredicateDef_And and) {
			conjunctions.addAll(and.getPredicates());
		} else {
			conjunctions.add(predicate);
		}

		List<Object> from = new ArrayList<>();
		List<Object> to = new ArrayList<>();

		L:
		for (String key : primaryKey) {
			for (int j = 0; j < conjunctions.size(); j++) {
				PredicateDef conjunction = conjunctions.get(j);
				if (conjunction instanceof PredicateDef_Eq eq && eq.getKey().equals(key)) {
					conjunctions.remove(j);
					from.add(toInternalValue(fields, eq.getKey(), eq.getValue()));
					to.add(toInternalValue(fields, eq.getKey(), eq.getValue()));
					continue L;
				}
				if (conjunction instanceof PredicateDef_Between between && between.getKey().equals(key)) {
					conjunctions.remove(j);
					from.add(toInternalValue(fields, between.getKey(), between.getFrom()));
					to.add(toInternalValue(fields, between.getKey(), between.getTo()));
					break L;
				}
			}
			break;
		}

		return RangeScan.rangeScan(PrimaryKey.ofList(from), PrimaryKey.ofList(to));
	}

}
