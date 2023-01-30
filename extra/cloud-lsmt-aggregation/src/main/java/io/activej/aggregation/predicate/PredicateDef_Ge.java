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
import io.activej.codegen.expression.Variable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.activej.aggregation.predicate.AggregationPredicates.isNotNull;
import static io.activej.aggregation.predicate.AggregationPredicates.toInternalValue;
import static io.activej.codegen.expression.Expressions.*;

public final class PredicateDef_Ge implements PredicateDef {
	private final String key;
	private final Comparable<Object> value;

	public PredicateDef_Ge(String key, Comparable<Object> value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public Comparable<Object> getValue() {
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

	@SuppressWarnings("rawtypes")
	@Override
	public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
		Variable property = property(record, key.replace('.', '$'));
		return and(
				isNotNull(property, fields.get(key)),
				isGe(property, value(toInternalValue(fields, key, value)))
		);
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
