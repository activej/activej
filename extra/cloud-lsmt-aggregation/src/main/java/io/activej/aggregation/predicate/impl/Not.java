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
import io.activej.aggregation.predicate.AggregationPredicates;
import io.activej.aggregation.predicate.PredicateDef;
import io.activej.codegen.expression.Expression;
import io.activej.common.annotation.ExposedInternals;

import java.util.Map;
import java.util.Set;

import static io.activej.codegen.expression.Expressions.not;

@ExposedInternals
public final class Not implements PredicateDef {
	public final PredicateDef predicate;

	public Not(PredicateDef predicate) {
		this.predicate = predicate;
	}

	@Override
	public PredicateDef simplify() {
		if (predicate instanceof Not not)
			return not.predicate.simplify();

		if (predicate instanceof Eq eq) {
			return new NotEq(eq.key, eq.value);}

		if (predicate instanceof NotEq notEq) {
			return new Eq(notEq.key, notEq.value);}

		if (predicate instanceof Gt gt)
			return new Le(gt.key, gt.value);

		if (predicate instanceof Lt lt)
			return new Ge(lt.key, lt.value);

		if (predicate instanceof Ge ge)
			return new Lt(ge.key, ge.value);

		if (predicate instanceof Le le)
			return new Gt(le.key, le.value);

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

		Not that = (Not) o;

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
