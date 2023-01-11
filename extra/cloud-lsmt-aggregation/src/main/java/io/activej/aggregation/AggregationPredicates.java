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

package io.activej.aggregation;

import io.activej.aggregation.fieldtype.FieldType;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.expression.Variable;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.regex.Pattern;

import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.first;
import static java.util.Collections.singletonMap;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationPredicates {
	private static final class E extends Expressions {}

	private record PredicateSimplifierKey<L extends AggregationPredicate, R extends AggregationPredicate>(
			Class<L> leftType, Class<R> rightType) {
	}

	@FunctionalInterface
	public interface PredicateSimplifier<L extends AggregationPredicate, R extends AggregationPredicate> {
		AggregationPredicate simplifyAnd(L left, R right);
	}

	private static final Map<PredicateSimplifierKey<?, ?>, PredicateSimplifier<?, ?>> simplifiers = new HashMap<>();

	public static <L extends AggregationPredicate, R extends AggregationPredicate> void register(Class<L> leftType, Class<R> rightType, PredicateSimplifier<L, R> operation) {
		PredicateSimplifierKey keyLeftRight = new PredicateSimplifierKey<>(leftType, rightType);
		checkState(!simplifiers.containsKey(keyLeftRight), "Key '%s has already been registered", keyLeftRight);
		simplifiers.put(keyLeftRight, operation);
		if (!rightType.equals(leftType)) {
			PredicateSimplifierKey keyRightLeft = new PredicateSimplifierKey<>(rightType, leftType);
			checkState(!simplifiers.containsKey(keyRightLeft), "Key '%s has already been registered", keyRightLeft);
			simplifiers.put(keyRightLeft, (PredicateSimplifier<R, L>) (right, left) -> operation.simplifyAnd(left, right));
		}
	}

	static {
		PredicateSimplifier simplifierAlwaysFalse = (PredicateSimplifier<AggregationPredicate_AlwaysFalse, AggregationPredicate>) (left, right) -> left;
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_AlwaysFalse.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_AlwaysTrue.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Not.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Eq.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_NotEq.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Le.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Ge.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Lt.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Gt.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Has.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Between.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_RegExp.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_And.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_Or.class, simplifierAlwaysFalse);
		register(AggregationPredicate_AlwaysFalse.class, AggregationPredicate_In.class, simplifierAlwaysFalse);

		PredicateSimplifier simplifierAlwaysTrue = (PredicateSimplifier<AggregationPredicate_AlwaysTrue, AggregationPredicate>) (left, right) -> right;
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_AlwaysTrue.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Not.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Eq.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_NotEq.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Le.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Ge.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Lt.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Gt.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Has.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Between.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_RegExp.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_And.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_Or.class, simplifierAlwaysTrue);
		register(AggregationPredicate_AlwaysTrue.class, AggregationPredicate_In.class, simplifierAlwaysTrue);

		PredicateSimplifier simplifierNot = (PredicateSimplifier<AggregationPredicate_Not, AggregationPredicate>) (left, right) -> {
			if (left.predicate.equals(right))
				return alwaysFalse();
			return null;
		};
		register(AggregationPredicate_Not.class, AggregationPredicate_Not.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_Has.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_Between.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_RegExp.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_And.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_Or.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_Ge.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_Le.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_Gt.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_Lt.class, simplifierNot);
		register(AggregationPredicate_Not.class, AggregationPredicate_In.class, simplifierNot);

		register(AggregationPredicate_Has.class, AggregationPredicate_Has.class, (left, right) -> left.key.equals(right.key) ? left : null);
		PredicateSimplifier simplifierHas = (PredicateSimplifier<AggregationPredicate_Has, AggregationPredicate>) (left, right) -> right.getDimensions().contains(left.getKey()) ? right : null;
		register(AggregationPredicate_Has.class, AggregationPredicate_Eq.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_NotEq.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_Le.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_Ge.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_Lt.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_Gt.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_Between.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_And.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_Or.class, simplifierHas);
		register(AggregationPredicate_Has.class, AggregationPredicate_In.class, simplifierHas);

		register(AggregationPredicate_Eq.class, AggregationPredicate_Eq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_NotEq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (!left.value.equals(right.value))
				return left;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_Le.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return left;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_Ge.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return left;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_Lt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) > 0)
				return left;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) < 0)
				return left;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_Between.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.from.compareTo(left.value) <= 0 && right.to.compareTo(left.value) >= 0)
				return left;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_RegExp.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value instanceof CharSequence &&
					right.regexp.matcher((CharSequence) left.value).matches())
				return left;
			return alwaysFalse();
		});
		register(AggregationPredicate_Eq.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.values.contains(left.value))
				return left;
			return alwaysFalse();
		});

		register(AggregationPredicate_NotEq.class, AggregationPredicate_NotEq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.equals(right.value))
				return left;
			return null;
		});
		register(AggregationPredicate_NotEq.class, AggregationPredicate_Le.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) < 0)
				return right;
			if (right.value.compareTo(left.value) == 0)
				return lt(left.key, right.value);
			return null;
		});
		register(AggregationPredicate_NotEq.class, AggregationPredicate_Ge.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) > 0)
				return right;
			if (right.value.compareTo(left.value) == 0)
				return gt(left.key, right.value);
			return null;
		});
		register(AggregationPredicate_NotEq.class, AggregationPredicate_Lt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return null;
		});
		register(AggregationPredicate_NotEq.class, AggregationPredicate_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return null;
		});
		register(AggregationPredicate_NotEq.class, AggregationPredicate_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.from.compareTo(left.value) > 0 && right.to.compareTo(left.value) > 0)
				return right;
			if (right.from.compareTo(left.value) < 0 && right.to.compareTo(left.value) < 0)
				return right;
			return null;
		});
		register(AggregationPredicate_NotEq.class, AggregationPredicate_RegExp.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value instanceof CharSequence &&
					right.regexp.matcher((CharSequence) left.value).matches())
				return null;
			return right;
		});
		register(AggregationPredicate_NotEq.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.values.contains(left.value))
				return alwaysFalse();
			return right;
		});

		register(AggregationPredicate_Le.class, AggregationPredicate_Le.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return left;
		});
		register(AggregationPredicate_Le.class, AggregationPredicate_Ge.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.value) < 0)
				return alwaysFalse();
			if (left.value.compareTo(right.value) > 0)
				return between(right.key, right.value, left.value);
			if (left.value.compareTo(right.value) == 0)
				return eq(left.key, left.value);
			return null;
		});
		register(AggregationPredicate_Le.class, AggregationPredicate_Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return left;
		});
		register(AggregationPredicate_Le.class, AggregationPredicate_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(AggregationPredicate_Le.class, AggregationPredicate_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.from.compareTo(left.value) > 0)
				return alwaysFalse();
			if (right.from.compareTo(left.value) == 0)
				return eq(left.key, right.from);
			if (right.to.compareTo(left.value) <= 0)
				return right;
			return between(right.key, right.from, left.value).simplify();
		});
		register(AggregationPredicate_Le.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.last()) >= 0)
				return right;
			if (left.value.compareTo(right.values.first()) < 0)
				return alwaysFalse();
			SortedSet subset = new TreeSet(right.values.headSet(left.value));
			if (right.values.contains(left.value)) subset.add(left.value);
			return in(left.key, subset);
		});

		register(AggregationPredicate_Ge.class, AggregationPredicate_Ge.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return left;
		});
		register(AggregationPredicate_Ge.class, AggregationPredicate_Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(AggregationPredicate_Ge.class, AggregationPredicate_Gt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return gt(right.key, right.value);
			return left;
		});
		register(AggregationPredicate_Ge.class, AggregationPredicate_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.to.compareTo(left.value) < 0)
				return alwaysFalse();
			if (right.to.compareTo(left.value) == 0)
				return eq(right.key, right.to);
			if (right.from.compareTo(left.value) >= 0)
				return right;
			return between(right.key, left.value, right.to).simplify();
		});
		register(AggregationPredicate_Ge.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.first()) <= 0)
				return right;
			if (left.value.compareTo(right.values.last()) > 0)
				return alwaysFalse();
			return in(left.key, new TreeSet(right.values.tailSet(left.value)));
		});

		register(AggregationPredicate_Lt.class, AggregationPredicate_Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return left;
			return right;
		});
		register(AggregationPredicate_Lt.class, AggregationPredicate_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(AggregationPredicate_Lt.class, AggregationPredicate_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.from.compareTo(left.value) >= 0)
				return alwaysFalse();
			if (right.to.compareTo(left.value) < 0)
				return right;
			return null;
		});
		register(AggregationPredicate_Lt.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.last()) > 0)
				return right;
			if (left.value.compareTo(right.values.first()) < 0)
				return alwaysFalse();
			return in(left.key, new TreeSet(right.values.subSet(right.values.first(), left.value)));
		});

		register(AggregationPredicate_Gt.class, AggregationPredicate_Gt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return left;
		});
		register(AggregationPredicate_Gt.class, AggregationPredicate_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.to.compareTo(left.value) <= 0)
				return alwaysFalse();
			if (right.from.compareTo(left.value) > 0)
				return right;
			return null;
		});
		register(AggregationPredicate_Gt.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.first()) < 0)
				return right;
			if (left.value.compareTo(right.values.last()) >= 0)
				return alwaysFalse();
			SortedSet subset = right.values.tailSet(left.value);
			subset.remove(left.value);
			return in(right.key, subset);
		});

		register(AggregationPredicate_Between.class, AggregationPredicate_Between.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			Comparable from = left.from.compareTo(right.from) >= 0 ? left.from : right.from;
			Comparable to = left.to.compareTo(right.to) <= 0 ? left.to : right.to;
			return between(left.key, from, to).simplify();
		});
		register(AggregationPredicate_Between.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.from.compareTo(right.values.first()) > 0 && left.to.compareTo(right.values.last()) > 0)
				return left;
			return null;
		});

		register(AggregationPredicate_In.class, AggregationPredicate_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.values.equals(right.values))
				return left.values.size() == 1 ? eq(left.getKey(), left.values.first()) : left;
			SortedSet values = left.values;
			values.retainAll(right.values);
			if (values.size() == 1)
				return eq(left.key, left.values.first());
			if (!left.values.isEmpty())
				return in(left.key, left.values);
			return alwaysFalse();
		});
	}

	public static final class AggregationPredicate_AlwaysFalse implements AggregationPredicate {
		private static final AggregationPredicate_AlwaysFalse instance = new AggregationPredicate_AlwaysFalse();

		private AggregationPredicate_AlwaysFalse() {
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of();
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			return E.value(false);
		}

		@Override
		public String toString() {
			return "FALSE";
		}
	}

	public static final class AggregationPredicate_AlwaysTrue implements AggregationPredicate {
		private static final AggregationPredicate_AlwaysTrue instance = new AggregationPredicate_AlwaysTrue();

		private AggregationPredicate_AlwaysTrue() {
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of();
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			return E.value(true);
		}

		@Override
		public String toString() {
			return "TRUE";
		}
	}

	public static final class AggregationPredicate_Not implements AggregationPredicate {
		private final AggregationPredicate predicate;

		private AggregationPredicate_Not(AggregationPredicate predicate) {
			this.predicate = predicate;
		}

		public AggregationPredicate getPredicate() {
			return predicate;
		}

		@Override
		public AggregationPredicate simplify() {
			if (predicate instanceof AggregationPredicate_Not)
				return ((AggregationPredicate_Not) predicate).predicate.simplify();

			if (predicate instanceof AggregationPredicate_Eq)
				return new AggregationPredicate_NotEq(((AggregationPredicate_Eq) predicate).key, ((AggregationPredicate_Eq) predicate).value);

			if (predicate instanceof AggregationPredicate_NotEq)
				return new AggregationPredicate_Eq(((AggregationPredicate_NotEq) predicate).key, ((AggregationPredicate_NotEq) predicate).value);

			if (predicate instanceof AggregationPredicate_Gt)
				return new AggregationPredicate_Le(((AggregationPredicate_Gt) predicate).key, ((AggregationPredicate_Gt) predicate).value);

			if (predicate instanceof AggregationPredicate_Lt)
				return new AggregationPredicate_Ge(((AggregationPredicate_Lt) predicate).key, ((AggregationPredicate_Lt) predicate).value);

			if (predicate instanceof AggregationPredicate_Ge)
				return new AggregationPredicate_Lt(((AggregationPredicate_Ge) predicate).key, ((AggregationPredicate_Ge) predicate).value);

			if (predicate instanceof AggregationPredicate_Le)
				return new AggregationPredicate_Gt(((AggregationPredicate_Le) predicate).key, ((AggregationPredicate_Le) predicate).value);

			return not(predicate.simplify());
		}

		@Override
		public Set<String> getDimensions() {
			return predicate.getDimensions();
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			return E.not(predicate.createPredicate(record, fields));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Not that = (AggregationPredicate_Not) o;

			return predicate.equals(that.predicate);

		}

		@Override
		public int hashCode() {
			return predicate.hashCode();
		}

		@Override
		public String toString() {
			return "NOT " + predicate;
		}
	}

	public static final class AggregationPredicate_Eq implements AggregationPredicate {
		final String key;
		final Object value;

		private AggregationPredicate_Eq(String key, Object value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public Object getValue() {
			return value;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return singletonMap(key, value);
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable property = E.property(record, key.replace('.', '$'));
			Object internalValue = toInternalValue(fields, key, this.value);
			return internalValue == null ?
					isNull(property, fields.get(key)) :
					E.and(
							isNotNull(property, fields.get(key)),
							E.isEq(property, E.value(internalValue)));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Eq that = (AggregationPredicate_Eq) o;

			if (!key.equals(that.key)) return false;
			return Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (value != null ? value.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return key + '=' + value;
		}
	}

	public static final class AggregationPredicate_NotEq implements AggregationPredicate {
		final String key;
		final Object value;

		private AggregationPredicate_NotEq(String key, Object value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public Object getValue() {
			return value;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable property = E.property(record, key.replace('.', '$'));
			Object internalValue = toInternalValue(fields, key, this.value);
			FieldType fieldType = fields.get(key);
			return internalValue == null ?
					isNotNull(property, fieldType) :
					E.or(
							isNull(property, fieldType),
							E.isNe(property, E.value(internalValue)));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_NotEq that = (AggregationPredicate_NotEq) o;

			if (!key.equals(that.key)) return false;
			return Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (value != null ? value.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return key + "!=" + value;
		}
	}

	public static final class AggregationPredicate_Le implements AggregationPredicate {
		final String key;
		final Comparable value;

		private AggregationPredicate_Le(String key, Comparable value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public Object getValue() {
			return value;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable property = E.property(record, key.replace('.', '$'));
			return E.and(
					isNotNull(property, fields.get(key)),
					E.isLe(property, E.value(toInternalValue(fields, key, value))));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Le that = (AggregationPredicate_Le) o;

			if (!key.equals(that.key)) return false;
			return Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (value != null ? value.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return key + "<=" + value;
		}
	}

	public static final class AggregationPredicate_Lt implements AggregationPredicate {
		final String key;
		final Comparable value;

		private AggregationPredicate_Lt(String key, Comparable value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public Object getValue() {
			return value;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable property = E.property(record, key.replace('.', '$'));
			return E.and(isNotNull(property, fields.get(key)),
					E.isLt(property, E.value(toInternalValue(fields, key, value))));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Lt that = (AggregationPredicate_Lt) o;

			if (!key.equals(that.key)) return false;
			return Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (value != null ? value.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return key + "<" + value;
		}
	}

	public static final class AggregationPredicate_Ge implements AggregationPredicate {
		final String key;
		final Comparable value;

		private AggregationPredicate_Ge(String key, Comparable value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public Comparable getValue() {
			return value;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable property = E.property(record, key.replace('.', '$'));
			return E.and(isNotNull(property, fields.get(key)),
					E.isGe(property, E.value(toInternalValue(fields, key, value))));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Ge that = (AggregationPredicate_Ge) o;

			if (!key.equals(that.key)) return false;
			return Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (value != null ? value.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return key + ">=" + value;
		}
	}

	public static final class AggregationPredicate_Gt implements AggregationPredicate {
		final String key;
		final Comparable value;

		private AggregationPredicate_Gt(String key, Comparable value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public Comparable getValue() {
			return value;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable property = E.property(record, key.replace('.', '$'));
			return E.and(isNotNull(property, fields.get(key)),
					E.isGt(property, E.value(toInternalValue(fields, key, value))));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Gt that = (AggregationPredicate_Gt) o;

			if (!key.equals(that.key)) return false;
			return Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (value != null ? value.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return key + ">" + value;
		}
	}

	public static final class AggregationPredicate_Has implements AggregationPredicate {
		final String key;

		private AggregationPredicate_Has(String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			if (!fields.containsKey(key)) return E.value(false);
			Variable property = E.property(record, key.replace('.', '$'));
			FieldType fieldType = fields.get(key);
			return isNotNull(property, fieldType);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Has that = (AggregationPredicate_Has) o;

			return key.equals(that.key);
		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result;
			return result;
		}

		@Override
		public String toString() {
			return "HAS " + key;
		}
	}

	public static final class AggregationPredicate_RegExp implements AggregationPredicate {
		final String key;
		final Pattern regexp;

		private AggregationPredicate_RegExp(String key, Pattern regexp) {
			this.key = key;
			this.regexp = regexp;
		}

		public String getKey() {
			return key;
		}

		public String getRegexp() {
			return regexp.pattern();
		}

		public Pattern getRegexpPattern() {
			return regexp;
		}

		@Override
		public AggregationPredicate simplify() {
			return this;
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable value = E.property(record, key.replace('.', '$'));
			return E.and(
					isNotNull(value, fields.get(key)),
					E.isNe(
							E.value(false),
							E.call(E.call(E.value(regexp), "matcher", toStringValue(fields, key, value)), "matches")));
		}

		private static Expression toStringValue(Map<String, FieldType> fields, String key, Expression value) {
			return fields.containsKey(key) ? fields.get(key).toStringValue(value) : value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_RegExp that = (AggregationPredicate_RegExp) o;

			if (!key.equals(that.key)) return false;
			return regexp.pattern().equals(that.regexp.pattern());

		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + regexp.pattern().hashCode();
			return result;
		}

		@Override
		public String toString() {
			return key + " " + regexp.pattern();
		}
	}

	public static final class AggregationPredicate_In implements AggregationPredicate {
		final String key;
		final SortedSet values;

		AggregationPredicate_In(String key, SortedSet values) {
			this.key = key;
			this.values = values;
		}

		public String getKey() {
			return key;
		}

		public Set getValues() {
			return values;
		}

		@Override
		public AggregationPredicate simplify() {
			return (values.iterator().hasNext()) ? this : alwaysFalse();
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			return E.isNe(
					E.value(false),
					E.call(E.value(values), "contains",
							E.cast(E.property(record, key.replace('.', '$')), Object.class)));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_In that = (AggregationPredicate_In) o;

			if (!key.equals(that.key)) return false;
			return Objects.equals(values, that.values);

		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (values != null ? values.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			StringJoiner joiner = new StringJoiner(", ");
			for (Object value : values) joiner.add(value != null ? value.toString() : null);
			return "" + key + " IN " + joiner;
		}
	}

	public static final class AggregationPredicate_Between implements AggregationPredicate {
		final String key;
		final Comparable from;
		final Comparable to;

		AggregationPredicate_Between(String key, Comparable from, Comparable to) {
			this.key = key;
			this.from = from;
			this.to = to;
		}

		public String getKey() {
			return key;
		}

		public Comparable getFrom() {
			return from;
		}

		public Comparable getTo() {
			return to;
		}

		@SuppressWarnings("unchecked")
		@Override
		public AggregationPredicate simplify() {
			return (from.compareTo(to) > 0) ? alwaysFalse() : (from.equals(to) ? AggregationPredicates.eq(key, from) : this);
		}

		@Override
		public Set<String> getDimensions() {
			return Set.of(key);
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			Variable property = E.property(record, key.replace('.', '$'));
			return E.and(isNotNull(property, fields.get(key)),
					E.isGe(property, E.value(toInternalValue(fields, key, from))),
					E.isLe(property, E.value(toInternalValue(fields, key, to))));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Between that = (AggregationPredicate_Between) o;

			if (!key.equals(that.key)) return false;
			if (!from.equals(that.from)) return false;
			return to.equals(that.to);

		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + from.hashCode();
			result = 31 * result + to.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "" + key + " BETWEEN " + from + " AND " + to;
		}
	}

	public static final class AggregationPredicate_And implements AggregationPredicate {
		final List<AggregationPredicate> predicates;

		private AggregationPredicate_And(List<AggregationPredicate> predicates) {
			this.predicates = predicates;
		}

		public List<AggregationPredicate> getPredicates() {
			return predicates;
		}

		@Override
		public AggregationPredicate simplify() {
			Set<AggregationPredicate> simplifiedPredicates = new LinkedHashSet<>();
			for (AggregationPredicate predicate : predicates) {
				AggregationPredicate simplified = predicate.simplify();
				if (simplified instanceof AggregationPredicate_And) {
					simplifiedPredicates.addAll(((AggregationPredicate_And) simplified).predicates);
				} else {
					simplifiedPredicates.add(simplified);
				}
			}
			boolean simplified;
			do {
				simplified = false;
				Set<AggregationPredicate> newPredicates = new HashSet<>();
				L:
				for (AggregationPredicate newPredicate : simplifiedPredicates) {
					for (AggregationPredicate simplifiedPredicate : newPredicates) {
						AggregationPredicate maybeSimplified = simplifyAnd(newPredicate, simplifiedPredicate);
						if (maybeSimplified != null) {
							newPredicates.remove(simplifiedPredicate);
							newPredicates.add(maybeSimplified);
							simplified = true;
							continue L;
						}
					}
					newPredicates.add(newPredicate);
				}
				simplifiedPredicates = newPredicates;
			} while (simplified);

			return simplifiedPredicates.isEmpty() ?
					alwaysTrue() :
					simplifiedPredicates.size() == 1 ?
							first(simplifiedPredicates) :
							and(new ArrayList<>(simplifiedPredicates));
		}

		@SuppressWarnings("unchecked")
		private static @Nullable AggregationPredicate simplifyAnd(AggregationPredicate left, AggregationPredicate right) {
			if (left.equals(right))
				return left;
			PredicateSimplifierKey key = new PredicateSimplifierKey(left.getClass(), right.getClass());
			PredicateSimplifier<AggregationPredicate, AggregationPredicate> simplifier = (PredicateSimplifier<AggregationPredicate, AggregationPredicate>) simplifiers.get(key);
			if (simplifier == null)
				return null;
			return simplifier.simplifyAnd(left, right);
		}

		@Override
		public Set<String> getDimensions() {
			Set<String> result = new HashSet<>();
			for (AggregationPredicate predicate : predicates) {
				result.addAll(predicate.getDimensions());
			}
			return result;
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			Map<String, Object> result = new HashMap<>();
			for (AggregationPredicate predicate : predicates) {
				result.putAll(predicate.getFullySpecifiedDimensions());
			}
			return result;
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			List<Expression> predicateDefs = new ArrayList<>();
			for (AggregationPredicate predicate : predicates) {
				predicateDefs.add(predicate.createPredicate(record, fields));
			}
			return E.and(predicateDefs);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_And that = (AggregationPredicate_And) o;

			return new HashSet<>(predicates).equals(new HashSet<>(that.predicates));

		}

		@Override
		public int hashCode() {
			return new HashSet<>(predicates).hashCode();
		}

		@Override
		public String toString() {
			StringJoiner joiner = new StringJoiner(" AND ");
			for (AggregationPredicate predicate : predicates)
				joiner.add(predicate != null ? predicate.toString() : null);

			return "(" + joiner + ")";
		}
	}

	public static final class AggregationPredicate_Or implements AggregationPredicate {
		final List<AggregationPredicate> predicates;

		AggregationPredicate_Or(List<AggregationPredicate> predicates) {
			this.predicates = predicates;
		}

		public List<AggregationPredicate> getPredicates() {
			return predicates;
		}

		@Override
		public AggregationPredicate simplify() {
			Set<AggregationPredicate> simplifiedPredicates = new LinkedHashSet<>();
			for (AggregationPredicate predicate : predicates) {
				AggregationPredicate simplified = predicate.simplify();
				if (simplified instanceof AggregationPredicate_Or) {
					simplifiedPredicates.addAll(((AggregationPredicate_Or) simplified).predicates);
				} else {
					simplifiedPredicates.add(simplified);
				}
			}
			return simplifiedPredicates.isEmpty() ?
					alwaysTrue() :
					simplifiedPredicates.size() == 1 ?
							first(simplifiedPredicates) :
							or(new ArrayList<>(simplifiedPredicates));
		}

		@Override
		public Set<String> getDimensions() {
			Set<String> result = new HashSet<>();
			for (AggregationPredicate predicate : predicates) {
				result.addAll(predicate.getDimensions());
			}
			return result;
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			return Map.of();
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			List<Expression> predicateDefs = new ArrayList<>();
			for (AggregationPredicate predicate : predicates) {
				predicateDefs.add(predicate.createPredicate(record, fields));
			}
			return E.or(predicateDefs);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AggregationPredicate_Or that = (AggregationPredicate_Or) o;

			return new HashSet<>(predicates).equals(new HashSet<>(that.predicates));

		}

		@Override
		public int hashCode() {
			return new HashSet<>(predicates).hashCode();
		}

		@Override
		public String toString() {
			StringJoiner joiner = new StringJoiner(" OR ");
			for (AggregationPredicate predicate : predicates)
				joiner.add(predicate != null ? predicate.toString() : null);
			return "(" + joiner + ")";
		}
	}

	public static AggregationPredicate alwaysTrue() {
		return AggregationPredicate_AlwaysTrue.instance;
	}

	public static AggregationPredicate alwaysFalse() {
		return AggregationPredicate_AlwaysFalse.instance;
	}

	public static AggregationPredicate not(AggregationPredicate predicate) {
		return new AggregationPredicate_Not(predicate);
	}

	public static AggregationPredicate and(List<AggregationPredicate> predicates) {
		return new AggregationPredicate_And(predicates);
	}

	public static AggregationPredicate and(AggregationPredicate... predicates) {
		return and(List.of(predicates));
	}

	public static AggregationPredicate or(List<AggregationPredicate> predicates) {
		return new AggregationPredicate_Or(predicates);
	}

	public static AggregationPredicate or(AggregationPredicate... predicates) {
		return or(List.of(predicates));
	}

	public static AggregationPredicate eq(String key, @Nullable Object value) {
		return new AggregationPredicate_Eq(key, value);
	}

	public static AggregationPredicate notEq(String key, Object value) {
		return new AggregationPredicate_NotEq(key, value);
	}

	public static AggregationPredicate ge(String key, Comparable value) {
		return new AggregationPredicate_Ge(key, value);
	}

	public static AggregationPredicate le(String key, Comparable value) {
		return new AggregationPredicate_Le(key, value);
	}

	public static AggregationPredicate gt(String key, Comparable value) {
		return new AggregationPredicate_Gt(key, value);
	}

	public static AggregationPredicate lt(String key, Comparable value) {
		return new AggregationPredicate_Lt(key, value);
	}

	public static AggregationPredicate has(String key) {
		return new AggregationPredicate_Has(key);
	}

	@SuppressWarnings("unchecked")
	public static AggregationPredicate in(String key, Collection values) {
		return values.size() == 1 ? new AggregationPredicate_Eq(key, values.toArray()[0]) : new AggregationPredicate_In(key, new TreeSet(values));
	}

	@SuppressWarnings("unchecked")
	public static AggregationPredicate in(String key, Comparable... values) {
		return values.length == 1 ? new AggregationPredicate_Eq(key, values[0]) : new AggregationPredicate_In(key, new TreeSet(List.of(values)));
	}

	public static AggregationPredicate regexp(String key, @Language("RegExp") String pattern) {
		return new AggregationPredicate_RegExp(key, Pattern.compile(pattern));
	}

	public static AggregationPredicate regexp(String key, Pattern pattern) {
		return new AggregationPredicate_RegExp(key, pattern);
	}

	public static AggregationPredicate between(String key, Comparable from, Comparable to) {
		return new AggregationPredicate_Between(key, from, to);
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

	private static Expression isNotNull(Expression field, FieldType fieldType) {
		return fieldType != null && fieldType.getInternalDataType().isPrimitive() ? E.value(true) : E.isNotNull(field);
	}

	private static Expression isNull(Expression field, FieldType fieldType) {
		return fieldType != null && fieldType.getInternalDataType().isPrimitive() ? E.value(false) : E.isNull(field);
	}

	@SuppressWarnings("unchecked")
	private static Object toInternalValue(Map<String, FieldType> fields, String key, Object value) {
		return fields.containsKey(key) ? fields.get(key).toInternalValue(value) : value;
	}

	public static RangeScan toRangeScan(AggregationPredicate predicate, List<String> primaryKey, Map<String, FieldType> fields) {
		predicate = predicate.simplify();
		if (predicate == alwaysFalse())
			return RangeScan.noScan();
		List<AggregationPredicate> conjunctions = new ArrayList<>();
		if (predicate instanceof AggregationPredicate_And) {
			conjunctions.addAll(((AggregationPredicate_And) predicate).predicates);
		} else {
			conjunctions.add(predicate);
		}

		List<Object> from = new ArrayList<>();
		List<Object> to = new ArrayList<>();

		L:
		for (String key : primaryKey) {
			for (int j = 0; j < conjunctions.size(); j++) {
				AggregationPredicate conjunction = conjunctions.get(j);
				if (conjunction instanceof AggregationPredicate_Eq eq && ((AggregationPredicate_Eq) conjunction).key.equals(key)) {
					conjunctions.remove(j);
					from.add(toInternalValue(fields, eq.key, eq.value));
					to.add(toInternalValue(fields, eq.key, eq.value));
					continue L;
				}
				if (conjunction instanceof AggregationPredicate_Between between && ((AggregationPredicate_Between) conjunction).key.equals(key)) {
					conjunctions.remove(j);
					from.add(toInternalValue(fields, between.key, between.from));
					to.add(toInternalValue(fields, between.key, between.to));
					break L;
				}
			}
			break;
		}

		return RangeScan.rangeScan(PrimaryKey.ofList(from), PrimaryKey.ofList(to));
	}

}
