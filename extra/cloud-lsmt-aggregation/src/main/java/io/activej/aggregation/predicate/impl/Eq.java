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
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExposedInternals;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.activej.aggregation.predicate.AggregationPredicates.isNotNull;
import static io.activej.aggregation.predicate.AggregationPredicates.isNull;
import static io.activej.aggregation.predicate.AggregationPredicates.*;
import static io.activej.codegen.expression.Expressions.and;
import static io.activej.codegen.expression.Expressions.*;
import static java.util.Collections.singletonMap;

@ExposedInternals
public final class Eq implements AggregationPredicate {
	public final String key;
	public final Object value;

	public Eq(String key, Object value) {
		this.key = key;
		this.value = value;
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

	@SuppressWarnings("rawtypes")
	@Override
	public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
		Variable property = property(record, key.replace('.', '$'));
		Object internalValue = toInternalValue(fields, key, this.value);
		return internalValue == null ?
				isNull(property, fields.get(key)) :
				and(
						isNotNull(property, fields.get(key)),
						isEq(property, value(internalValue))
				);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Eq that = (Eq) o;

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
