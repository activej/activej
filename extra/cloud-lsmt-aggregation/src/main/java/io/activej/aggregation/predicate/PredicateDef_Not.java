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

import io.activej.aggregation.fieldtype.FieldType;
import io.activej.codegen.expression.Expression;

import java.util.Map;
import java.util.Set;

import static io.activej.codegen.expression.Expressions.not;

final class PredicateDef_Not implements PredicateDef {
	private final PredicateDef predicate;

	PredicateDef_Not(PredicateDef predicate) {
		this.predicate = predicate;
	}

	PredicateDef getPredicate() {
		return predicate;
	}

	@Override
	public PredicateDef simplify() {
		if (predicate instanceof PredicateDef_Not not)
			return not.predicate.simplify();

		if (predicate instanceof PredicateDef_Eq eq)
			return new PredicateDef_NotEq(eq.getKey(), eq.getValue());

		if (predicate instanceof PredicateDef_NotEq notEq)
			return new PredicateDef_Eq(notEq.getKey(), notEq.getValue());

		if (predicate instanceof PredicateDef_Gt gt)
			return new PredicateDef_Le(gt.getKey(), gt.getValue());

		if (predicate instanceof PredicateDef_Lt lt)
			return new PredicateDef_Ge(lt.getKey(), lt.getValue());

		if (predicate instanceof PredicateDef_Ge ge)
			return new PredicateDef_Lt(ge.getKey(), ge.getValue());

		if (predicate instanceof PredicateDef_Le le)
			return new PredicateDef_Gt(le.getKey(), le.getValue());

		return AggregationPredicates.not(predicate.simplify());
	}

	@Override
	public Set<String> getDimensions() {
		return predicate.getDimensions();
	}

	@Override
	public Map<String, Object> getFullySpecifiedDimensions() {
		return Map.of();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
		return not(predicate.createPredicate(record, fields));
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
