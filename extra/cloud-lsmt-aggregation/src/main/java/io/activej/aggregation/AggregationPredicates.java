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

	private record PredicateSimplifierKey<L extends PredicateDef, R extends PredicateDef>(
			Class<L> leftType, Class<R> rightType) {
	}

	@FunctionalInterface
	public interface PredicateSimplifier<L extends PredicateDef, R extends PredicateDef> {
		PredicateDef simplifyAnd(L left, R right);
	}

	private static final Map<PredicateSimplifierKey<?, ?>, PredicateSimplifier<?, ?>> simplifiers = new HashMap<>();

	public static <L extends PredicateDef, R extends PredicateDef> void register(Class<L> leftType, Class<R> rightType, PredicateSimplifier<L, R> operation) {
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
			if (left.predicate.equals(right))
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

		register(PredicateDef_Has.class, PredicateDef_Has.class, (left, right) -> left.key.equals(right.key) ? left : null);
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
			if (!left.key.equals(right.key))
				return null;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_NotEq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (!left.value.equals(right.value))
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Le.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Ge.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Lt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) > 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) < 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_Between.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.from.compareTo(left.value) <= 0 && right.to.compareTo(left.value) >= 0)
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_RegExp.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value instanceof CharSequence &&
					right.regexp.matcher((CharSequence) left.value).matches())
				return left;
			return alwaysFalse();
		});
		register(PredicateDef_Eq.class, PredicateDef_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.values.contains(left.value))
				return left;
			return alwaysFalse();
		});

		register(PredicateDef_NotEq.class, PredicateDef_NotEq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.equals(right.value))
				return left;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Le.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) < 0)
				return right;
			if (right.value.compareTo(left.value) == 0)
				return lt(left.key, right.value);
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Ge.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) > 0)
				return right;
			if (right.value.compareTo(left.value) == 0)
				return gt(left.key, right.value);
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Lt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.from.compareTo(left.value) > 0 && right.to.compareTo(left.value) > 0)
				return right;
			if (right.from.compareTo(left.value) < 0 && right.to.compareTo(left.value) < 0)
				return right;
			return null;
		});
		register(PredicateDef_NotEq.class, PredicateDef_RegExp.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value instanceof CharSequence &&
					right.regexp.matcher((CharSequence) left.value).matches())
				return null;
			return right;
		});
		register(PredicateDef_NotEq.class, PredicateDef_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.values.contains(left.value))
				return alwaysFalse();
			return right;
		});

		register(PredicateDef_Le.class, PredicateDef_Le.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return left;
		});
		register(PredicateDef_Le.class, PredicateDef_Ge.class, (left, right) -> {
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
		register(PredicateDef_Le.class, PredicateDef_Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return left;
		});
		register(PredicateDef_Le.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(PredicateDef_Le.class, PredicateDef_Between.class, (left, right) -> {
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
		register(PredicateDef_Le.class, PredicateDef_In.class, (left, right) -> {
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

		register(PredicateDef_Ge.class, PredicateDef_Ge.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return left;
		});
		register(PredicateDef_Ge.class, PredicateDef_Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(PredicateDef_Ge.class, PredicateDef_Gt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return gt(right.key, right.value);
			return left;
		});
		register(PredicateDef_Ge.class, PredicateDef_Between.class, (left, right) -> {
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
		register(PredicateDef_Ge.class, PredicateDef_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.first()) <= 0)
				return right;
			if (left.value.compareTo(right.values.last()) > 0)
				return alwaysFalse();
			return in(left.key, new TreeSet(right.values.tailSet(left.value)));
		});

		register(PredicateDef_Lt.class, PredicateDef_Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return left;
			return right;
		});
		register(PredicateDef_Lt.class, PredicateDef_Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(PredicateDef_Lt.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.from.compareTo(left.value) >= 0)
				return alwaysFalse();
			if (right.to.compareTo(left.value) < 0)
				return right;
			return null;
		});
		register(PredicateDef_Lt.class, PredicateDef_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.last()) > 0)
				return right;
			if (left.value.compareTo(right.values.first()) < 0)
				return alwaysFalse();
			return in(left.key, new TreeSet(right.values.subSet(right.values.first(), left.value)));
		});

		register(PredicateDef_Gt.class, PredicateDef_Gt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return left;
		});
		register(PredicateDef_Gt.class, PredicateDef_Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.to.compareTo(left.value) <= 0)
				return alwaysFalse();
			if (right.from.compareTo(left.value) > 0)
				return right;
			return null;
		});
		register(PredicateDef_Gt.class, PredicateDef_In.class, (left, right) -> {
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

		register(PredicateDef_Between.class, PredicateDef_Between.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			Comparable from = left.from.compareTo(right.from) >= 0 ? left.from : right.from;
			Comparable to = left.to.compareTo(right.to) <= 0 ? left.to : right.to;
			return between(left.key, from, to).simplify();
		});
		register(PredicateDef_Between.class, PredicateDef_In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.from.compareTo(right.values.first()) > 0 && left.to.compareTo(right.values.last()) > 0)
				return left;
			return null;
		});

		register(PredicateDef_In.class, PredicateDef_In.class, (left, right) -> {
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

	public static final class PredicateDef_AlwaysFalse implements PredicateDef {
		private static final PredicateDef_AlwaysFalse instance = new PredicateDef_AlwaysFalse();

		private PredicateDef_AlwaysFalse() {
		}

		@Override
		public PredicateDef simplify() {
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

	public static final class PredicateDef_AlwaysTrue implements PredicateDef {
		private static final PredicateDef_AlwaysTrue instance = new PredicateDef_AlwaysTrue();

		private PredicateDef_AlwaysTrue() {
		}

		@Override
		public PredicateDef simplify() {
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

	public static final class PredicateDef_Not implements PredicateDef {
		private final PredicateDef predicate;

		private PredicateDef_Not(PredicateDef predicate) {
			this.predicate = predicate;
		}

		public PredicateDef getPredicate() {
			return predicate;
		}

		@Override
		public PredicateDef simplify() {
			if (predicate instanceof PredicateDef_Not)
				return ((PredicateDef_Not) predicate).predicate.simplify();

			if (predicate instanceof PredicateDef_Eq)
				return new PredicateDef_NotEq(((PredicateDef_Eq) predicate).key, ((PredicateDef_Eq) predicate).value);

			if (predicate instanceof PredicateDef_NotEq)
				return new PredicateDef_Eq(((PredicateDef_NotEq) predicate).key, ((PredicateDef_NotEq) predicate).value);

			if (predicate instanceof PredicateDef_Gt)
				return new PredicateDef_Le(((PredicateDef_Gt) predicate).key, ((PredicateDef_Gt) predicate).value);

			if (predicate instanceof PredicateDef_Lt)
				return new PredicateDef_Ge(((PredicateDef_Lt) predicate).key, ((PredicateDef_Lt) predicate).value);

			if (predicate instanceof PredicateDef_Ge)
				return new PredicateDef_Lt(((PredicateDef_Ge) predicate).key, ((PredicateDef_Ge) predicate).value);

			if (predicate instanceof PredicateDef_Le)
				return new PredicateDef_Gt(((PredicateDef_Le) predicate).key, ((PredicateDef_Le) predicate).value);

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

			PredicateDef_Not that = (PredicateDef_Not) o;

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

	public static final class PredicateDef_Eq implements PredicateDef {
		final String key;
		final Object value;

		private PredicateDef_Eq(String key, Object value) {
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
		public PredicateDef simplify() {
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

			PredicateDef_Eq that = (PredicateDef_Eq) o;

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

	public static final class PredicateDef_NotEq implements PredicateDef {
		final String key;
		final Object value;

		private PredicateDef_NotEq(String key, Object value) {
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
		public PredicateDef simplify() {
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

			PredicateDef_NotEq that = (PredicateDef_NotEq) o;

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

	public static final class PredicateDef_Le implements PredicateDef {
		final String key;
		final Comparable value;

		private PredicateDef_Le(String key, Comparable value) {
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
		public PredicateDef simplify() {
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

			PredicateDef_Le that = (PredicateDef_Le) o;

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

	public static final class PredicateDef_Lt implements PredicateDef {
		final String key;
		final Comparable value;

		private PredicateDef_Lt(String key, Comparable value) {
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
		public PredicateDef simplify() {
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

			PredicateDef_Lt that = (PredicateDef_Lt) o;

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

	public static final class PredicateDef_Ge implements PredicateDef {
		final String key;
		final Comparable value;

		private PredicateDef_Ge(String key, Comparable value) {
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
		public PredicateDef simplify() {
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

			PredicateDef_Ge that = (PredicateDef_Ge) o;

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

	public static final class PredicateDef_Gt implements PredicateDef {
		final String key;
		final Comparable value;

		private PredicateDef_Gt(String key, Comparable value) {
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
		public PredicateDef simplify() {
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

			PredicateDef_Gt that = (PredicateDef_Gt) o;

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

	public static final class PredicateDef_Has implements PredicateDef {
		final String key;

		private PredicateDef_Has(String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}

		@Override
		public PredicateDef simplify() {
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

			PredicateDef_Has that = (PredicateDef_Has) o;

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

	public static final class PredicateDef_RegExp implements PredicateDef {
		final String key;
		final Pattern regexp;

		private PredicateDef_RegExp(String key, Pattern regexp) {
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
		public PredicateDef simplify() {
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

			PredicateDef_RegExp that = (PredicateDef_RegExp) o;

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

	public static final class PredicateDef_In implements PredicateDef {
		final String key;
		final SortedSet values;

		PredicateDef_In(String key, SortedSet values) {
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
		public PredicateDef simplify() {
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

			PredicateDef_In that = (PredicateDef_In) o;

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

	public static final class PredicateDef_Between implements PredicateDef {
		final String key;
		final Comparable from;
		final Comparable to;

		PredicateDef_Between(String key, Comparable from, Comparable to) {
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
		public PredicateDef simplify() {
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

			PredicateDef_Between that = (PredicateDef_Between) o;

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

	public static final class PredicateDef_And implements PredicateDef {
		final List<PredicateDef> predicates;

		private PredicateDef_And(List<PredicateDef> predicates) {
			this.predicates = predicates;
		}

		public List<PredicateDef> getPredicates() {
			return predicates;
		}

		@Override
		public PredicateDef simplify() {
			Set<PredicateDef> simplifiedPredicates = new LinkedHashSet<>();
			for (PredicateDef predicate : predicates) {
				PredicateDef simplified = predicate.simplify();
				if (simplified instanceof PredicateDef_And) {
					simplifiedPredicates.addAll(((PredicateDef_And) simplified).predicates);
				} else {
					simplifiedPredicates.add(simplified);
				}
			}
			boolean simplified;
			do {
				simplified = false;
				Set<PredicateDef> newPredicates = new HashSet<>();
				L:
				for (PredicateDef newPredicate : simplifiedPredicates) {
					for (PredicateDef simplifiedPredicate : newPredicates) {
						PredicateDef maybeSimplified = simplifyAnd(newPredicate, simplifiedPredicate);
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
		private static @Nullable PredicateDef simplifyAnd(PredicateDef left, PredicateDef right) {
			if (left.equals(right))
				return left;
			PredicateSimplifierKey key = new PredicateSimplifierKey(left.getClass(), right.getClass());
			PredicateSimplifier<PredicateDef, PredicateDef> simplifier = (PredicateSimplifier<PredicateDef, PredicateDef>) simplifiers.get(key);
			if (simplifier == null)
				return null;
			return simplifier.simplifyAnd(left, right);
		}

		@Override
		public Set<String> getDimensions() {
			Set<String> result = new HashSet<>();
			for (PredicateDef predicate : predicates) {
				result.addAll(predicate.getDimensions());
			}
			return result;
		}

		@Override
		public Map<String, Object> getFullySpecifiedDimensions() {
			Map<String, Object> result = new HashMap<>();
			for (PredicateDef predicate : predicates) {
				result.putAll(predicate.getFullySpecifiedDimensions());
			}
			return result;
		}

		@Override
		public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
			List<Expression> predicateDefs = new ArrayList<>();
			for (PredicateDef predicate : predicates) {
				predicateDefs.add(predicate.createPredicate(record, fields));
			}
			return E.and(predicateDefs);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateDef_And that = (PredicateDef_And) o;

			return new HashSet<>(predicates).equals(new HashSet<>(that.predicates));

		}

		@Override
		public int hashCode() {
			return new HashSet<>(predicates).hashCode();
		}

		@Override
		public String toString() {
			StringJoiner joiner = new StringJoiner(" AND ");
			for (PredicateDef predicate : predicates)
				joiner.add(predicate != null ? predicate.toString() : null);

			return "(" + joiner + ")";
		}
	}

	public static final class PredicateDef_Or implements PredicateDef {
		final List<PredicateDef> predicates;

		PredicateDef_Or(List<PredicateDef> predicates) {
			this.predicates = predicates;
		}

		public List<PredicateDef> getPredicates() {
			return predicates;
		}

		@Override
		public PredicateDef simplify() {
			Set<PredicateDef> simplifiedPredicates = new LinkedHashSet<>();
			for (PredicateDef predicate : predicates) {
				PredicateDef simplified = predicate.simplify();
				if (simplified instanceof PredicateDef_Or) {
					simplifiedPredicates.addAll(((PredicateDef_Or) simplified).predicates);
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
			for (PredicateDef predicate : predicates) {
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
			for (PredicateDef predicate : predicates) {
				predicateDefs.add(predicate.createPredicate(record, fields));
			}
			return E.or(predicateDefs);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateDef_Or that = (PredicateDef_Or) o;

			return new HashSet<>(predicates).equals(new HashSet<>(that.predicates));

		}

		@Override
		public int hashCode() {
			return new HashSet<>(predicates).hashCode();
		}

		@Override
		public String toString() {
			StringJoiner joiner = new StringJoiner(" OR ");
			for (PredicateDef predicate : predicates)
				joiner.add(predicate != null ? predicate.toString() : null);
			return "(" + joiner + ")";
		}
	}

	public static PredicateDef alwaysTrue() {
		return PredicateDef_AlwaysTrue.instance;
	}

	public static PredicateDef alwaysFalse() {
		return PredicateDef_AlwaysFalse.instance;
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

	public static PredicateDef between(String key, Comparable from, Comparable to) {
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

	public static RangeScan toRangeScan(PredicateDef predicate, List<String> primaryKey, Map<String, FieldType> fields) {
		predicate = predicate.simplify();
		if (predicate == alwaysFalse())
			return RangeScan.noScan();
		List<PredicateDef> conjunctions = new ArrayList<>();
		if (predicate instanceof PredicateDef_And) {
			conjunctions.addAll(((PredicateDef_And) predicate).predicates);
		} else {
			conjunctions.add(predicate);
		}

		List<Object> from = new ArrayList<>();
		List<Object> to = new ArrayList<>();

		L:
		for (String key : primaryKey) {
			for (int j = 0; j < conjunctions.size(); j++) {
				PredicateDef conjunction = conjunctions.get(j);
				if (conjunction instanceof PredicateDef_Eq eq && ((PredicateDef_Eq) conjunction).key.equals(key)) {
					conjunctions.remove(j);
					from.add(toInternalValue(fields, eq.key, eq.value));
					to.add(toInternalValue(fields, eq.key, eq.value));
					continue L;
				}
				if (conjunction instanceof PredicateDef_Between between && ((PredicateDef_Between) conjunction).key.equals(key)) {
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
