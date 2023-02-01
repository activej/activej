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
import io.activej.aggregation.predicate.impl.*;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.common.annotation.StaticFactories;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.regex.Pattern;

import static io.activej.codegen.expression.Expressions.value;
import static io.activej.common.Checks.checkState;

@SuppressWarnings({"rawtypes", "unchecked"})
@StaticFactories(PredicateDef.class)
public class AggregationPredicates {

	public record PredicateSimplifierKey<L extends PredicateDef, R extends PredicateDef>(
			Class<L> leftType, Class<R> rightType) {
	}

	@FunctionalInterface
	public interface PredicateSimplifier<L extends PredicateDef, R extends PredicateDef> {
		PredicateDef simplifyAnd(L left, R right);
	}

	public static final Map<PredicateSimplifierKey<?, ?>, PredicateSimplifier<?, ?>> simplifiers = new HashMap<>();

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
		PredicateSimplifier simplifierAlwaysFalse = (PredicateSimplifier<AlwaysFalse, PredicateDef>) (left, right) -> left;
		register(AlwaysFalse.class, AlwaysFalse.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, AlwaysTrue.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Not.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Eq.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, NotEq.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Le.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Ge.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Lt.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Gt.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Has.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Between.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, RegExp.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, And.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, Or.class, simplifierAlwaysFalse);
		register(AlwaysFalse.class, In.class, simplifierAlwaysFalse);

		PredicateSimplifier simplifierAlwaysTrue = (PredicateSimplifier<AlwaysTrue, PredicateDef>) (left, right) -> right;
		register(AlwaysTrue.class, AlwaysTrue.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Not.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Eq.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, NotEq.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Le.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Ge.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Lt.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Gt.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Has.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Between.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, RegExp.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, And.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, Or.class, simplifierAlwaysTrue);
		register(AlwaysTrue.class, In.class, simplifierAlwaysTrue);

		PredicateSimplifier simplifierNot = (PredicateSimplifier<Not, PredicateDef>) (left, right) -> {
			if (left.predicate.equals(right))
				return alwaysFalse();
			return null;
		};
		register(Not.class, Not.class, simplifierNot);
		register(Not.class, Has.class, simplifierNot);
		register(Not.class, Between.class, simplifierNot);
		register(Not.class, RegExp.class, simplifierNot);
		register(Not.class, And.class, simplifierNot);
		register(Not.class, Or.class, simplifierNot);
		register(Not.class, Ge.class, simplifierNot);
		register(Not.class, Le.class, simplifierNot);
		register(Not.class, Gt.class, simplifierNot);
		register(Not.class, Lt.class, simplifierNot);
		register(Not.class, In.class, simplifierNot);

		register(Has.class, Has.class, (left, right) -> left.key.equals(right.key) ? left : null);
		PredicateSimplifier simplifierHas = (PredicateSimplifier<Has, PredicateDef>) (left, right) -> right.getDimensions().contains(left.key) ? right : null;
		register(Has.class, Eq.class, simplifierHas);
		register(Has.class, NotEq.class, simplifierHas);
		register(Has.class, Le.class, simplifierHas);
		register(Has.class, Ge.class, simplifierHas);
		register(Has.class, Lt.class, simplifierHas);
		register(Has.class, Gt.class, simplifierHas);
		register(Has.class, Between.class, simplifierHas);
		register(Has.class, And.class, simplifierHas);
		register(Has.class, Or.class, simplifierHas);
		register(Has.class, In.class, simplifierHas);

		register(Eq.class, Eq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			return alwaysFalse();
		});
		register(Eq.class, NotEq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (!left.value.equals(right.value))
				return left;
			return alwaysFalse();
		});
		register(Eq.class, Le.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return left;
			return alwaysFalse();
		});
		register(Eq.class, Ge.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return left;
			return alwaysFalse();
		});
		register(Eq.class, Lt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) > 0)
				return left;
			return alwaysFalse();
		});
		register(Eq.class, Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) < 0)
				return left;
			return alwaysFalse();
		});
		register(Eq.class, Between.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.from.compareTo(left.value) <= 0 && right.to.compareTo(left.value) >= 0)
				return left;
			return alwaysFalse();
		});
		register(Eq.class, RegExp.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value instanceof CharSequence sequence && right.regexp.matcher(sequence).matches())
				return left;
			return alwaysFalse();
		});
		register(Eq.class, In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.values.contains(left.value))
				return left;
			return alwaysFalse();
		});

		register(NotEq.class, NotEq.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.equals(right.value))
				return left;
			return null;
		});
		register(NotEq.class, Le.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) < 0)
				return right;
			if (right.value.compareTo(left.value) == 0)
				return lt(left.key, right.value);
			return null;
		});
		register(NotEq.class, Ge.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) > 0)
				return right;
			if (right.value.compareTo(left.value) == 0)
				return gt(left.key, right.value);
			return null;
		});
		register(NotEq.class, Lt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return null;
		});
		register(NotEq.class, Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return null;
		});
		register(NotEq.class, Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.from.compareTo(left.value) > 0 && right.to.compareTo(left.value) > 0)
				return right;
			if (right.from.compareTo(left.value) < 0 && right.to.compareTo(left.value) < 0)
				return right;
			return null;
		});
		register(NotEq.class, RegExp.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value instanceof CharSequence sequence && right.regexp.matcher(sequence).matches())
				return null;
			return right;
		});
		register(NotEq.class, In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (right.values.contains(left.value))
				return alwaysFalse();
			return right;
		});

		register(Le.class, Le.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return left;
		});
		register(Le.class, Ge.class, (left, right) -> {
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
		register(Le.class, Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return right;
			return left;
		});
		register(Le.class, Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(Le.class, Between.class, (left, right) -> {
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
		register(Le.class, In.class, (left, right) -> {
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

		register(Ge.class, Ge.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return left;
		});
		register(Ge.class, Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(Ge.class, Gt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return gt(right.key, right.value);
			return left;
		});
		register(Ge.class, Between.class, (left, right) -> {
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
		register(Ge.class, In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.first()) <= 0)
				return right;
			if (left.value.compareTo(right.values.last()) > 0)
				return alwaysFalse();
			return in(left.key, new TreeSet(right.values.tailSet(left.value)));
		});

		register(Lt.class, Lt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return left;
			return right;
		});
		register(Lt.class, Gt.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.value) <= 0)
				return alwaysFalse();
			return null;
		});
		register(Lt.class, Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.from.compareTo(left.value) >= 0)
				return alwaysFalse();
			if (right.to.compareTo(left.value) < 0)
				return right;
			return null;
		});
		register(Lt.class, In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.value.compareTo(right.values.last()) > 0)
				return right;
			if (left.value.compareTo(right.values.first()) < 0)
				return alwaysFalse();
			return in(left.key, new TreeSet(right.values.subSet(right.values.first(), left.value)));
		});

		register(Gt.class, Gt.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.value.compareTo(left.value) >= 0)
				return right;
			return left;
		});
		register(Gt.class, Between.class, (left, right) -> {
			if (!right.key.equals(left.key))
				return null;
			if (right.to.compareTo(left.value) <= 0)
				return alwaysFalse();
			if (right.from.compareTo(left.value) > 0)
				return right;
			return null;
		});
		register(Gt.class, In.class, (left, right) -> {
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

		register(Between.class, Between.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			Comparable from = left.from.compareTo(right.from) >= 0 ? left.from : right.from;
			Comparable to = left.to.compareTo(right.to) <= 0 ? left.to : right.to;
			return between(left.key, from, to).simplify();
		});
		register(Between.class, In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.from.compareTo(right.values.first()) > 0 && left.to.compareTo(right.values.last()) > 0)
				return left;
			return null;
		});

		register(In.class, In.class, (left, right) -> {
			if (!left.key.equals(right.key))
				return null;
			if (left.values.equals(right.values))
				return left.values.size() == 1 ? eq(left.key, left.values.first()) : left;
			SortedSet values = left.values;
			values.retainAll(right.values);
			if (values.size() == 1)
				return eq(left.key, left.values.first());
			if (!left.values.isEmpty())
				return in(left.key, left.values);
			return alwaysFalse();
		});
	}

	public static PredicateDef alwaysTrue() {
		return AlwaysTrue.INSTANCE;
	}

	public static PredicateDef alwaysFalse() {
		return AlwaysFalse.INSTANCE;
	}

	public static PredicateDef not(PredicateDef predicate) {
		return new Not(predicate);
	}

	public static PredicateDef and(List<PredicateDef> predicates) {
		return new And(predicates);
	}

	public static PredicateDef and(PredicateDef... predicates) {
		return and(List.of(predicates));
	}

	public static PredicateDef or(List<PredicateDef> predicates) {
		return new Or(predicates);
	}

	public static PredicateDef or(PredicateDef... predicates) {
		return or(List.of(predicates));
	}

	public static PredicateDef eq(String key, @Nullable Object value) {
		return new Eq(key, value);
	}

	public static PredicateDef notEq(String key, Object value) {
		return new NotEq(key, value);
	}

	public static PredicateDef ge(String key, Comparable value) {
		return new Ge(key, value);
	}

	public static PredicateDef le(String key, Comparable value) {
		return new Le(key, value);
	}

	public static PredicateDef gt(String key, Comparable value) {
		return new Gt(key, value);
	}

	public static PredicateDef lt(String key, Comparable value) {
		return new Lt(key, value);
	}

	public static PredicateDef has(String key) {
		return new Has(key);
	}

	@SuppressWarnings("unchecked")
	public static PredicateDef in(String key, Collection values) {
		return values.size() == 1 ? new Eq(key, values.toArray()[0]) : new In(key, new TreeSet(values));
	}

	@SuppressWarnings("unchecked")
	public static PredicateDef in(String key, Comparable... values) {
		return values.length == 1 ? new Eq(key, values[0]) : new In(key, new TreeSet(List.of(values)));
	}

	public static PredicateDef regexp(String key, @Language("RegExp") String pattern) {
		return new RegExp(key, Pattern.compile(pattern));
	}

	public static PredicateDef regexp(String key, Pattern pattern) {
		return new RegExp(key, pattern);
	}

	public static <C extends Comparable<C>> PredicateDef between(String key, Comparable from, Comparable to) {
		return new Between(key, from, to);
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

	public static Expression isNotNull(Expression field, FieldType fieldType) {
		return fieldType != null && fieldType.getInternalDataType().isPrimitive() ? value(true) : Expressions.isNotNull(field);
	}

	public static Expression isNull(Expression field, FieldType fieldType) {
		return fieldType != null && fieldType.getInternalDataType().isPrimitive() ? value(false) : Expressions.isNull(field);
	}

	@SuppressWarnings("unchecked")
	public static Object toInternalValue(Map<String, FieldType> fields, String key, Object value) {
		return fields.containsKey(key) ? fields.get(key).toInternalValue(value) : value;
	}

	public static RangeScan toRangeScan(PredicateDef predicate, List<String> primaryKey, Map<String, FieldType> fields) {
		predicate = predicate.simplify();
		if (predicate == alwaysFalse())
			return RangeScan.noScan();
		List<PredicateDef> conjunctions = new ArrayList<>();
		if (predicate instanceof And and) {
			conjunctions.addAll(and.predicates);
		} else {
			conjunctions.add(predicate);
		}

		List<Object> from = new ArrayList<>();
		List<Object> to = new ArrayList<>();

		L:
		for (String key : primaryKey) {
			for (int j = 0; j < conjunctions.size(); j++) {
				PredicateDef conjunction = conjunctions.get(j);
				if (conjunction instanceof Eq eq && eq.key.equals(key)) {
					conjunctions.remove(j);
					from.add(toInternalValue(fields, eq.key, eq.value));
					to.add(toInternalValue(fields, eq.key, eq.value));
					continue L;
				}
				if (conjunction instanceof Between between && between.key.equals(key)) {
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
