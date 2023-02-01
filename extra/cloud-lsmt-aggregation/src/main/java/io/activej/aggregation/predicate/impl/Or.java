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

package io.activej.aggregation.predicate.impl;

import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.predicate.AggregationPredicate;
import io.activej.aggregation.predicate.AggregationPredicates;
import io.activej.codegen.expression.Expression;
import io.activej.common.annotation.ExposedInternals;

import java.util.*;

import static io.activej.codegen.expression.Expressions.or;
import static io.activej.common.Utils.first;

@ExposedInternals
public final class Or implements AggregationPredicate {
	public final List<AggregationPredicate> predicates;

	public Or(List<AggregationPredicate> predicates) {
		this.predicates = predicates;
	}

	@Override
	public AggregationPredicate simplify() {
		Set<AggregationPredicate> simplifiedPredicates = new LinkedHashSet<>();
		for (AggregationPredicate predicate : predicates) {
			AggregationPredicate simplified = predicate.simplify();
			if (simplified instanceof Or or) {
				simplifiedPredicates.addAll(or.predicates);
			} else {
				simplifiedPredicates.add(simplified);
			}
		}
		return simplifiedPredicates.isEmpty() ?
				AggregationPredicates.alwaysTrue() :
				simplifiedPredicates.size() == 1 ?
						first(simplifiedPredicates) :
						AggregationPredicates.or(new ArrayList<>(simplifiedPredicates));
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

	@SuppressWarnings("rawtypes")
	@Override
	public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
		List<Expression> predicateDefs = new ArrayList<>();
		for (AggregationPredicate predicate : predicates) {
			predicateDefs.add(predicate.createPredicate(record, fields));
		}
		return or(predicateDefs);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Or that = (Or) o;

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
