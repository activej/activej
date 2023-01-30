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
import java.util.Set;

import static io.activej.aggregation.predicate.AggregationPredicates.isNotNull;
import static io.activej.codegen.expression.Expressions.property;
import static io.activej.codegen.expression.Expressions.value;

public final class PredicateDef_Has implements PredicateDef {
	private final String key;

	PredicateDef_Has(String key) {
		this.key = key;
	}

	String getKey() {
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

	@SuppressWarnings("rawtypes")
	@Override
	public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
		if (!fields.containsKey(key)) return value(false);
		Variable property = property(record, key.replace('.', '$'));
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
