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

import java.util.*;

import static io.activej.codegen.expression.Expressions.*;

public final class PredicateDef_In implements PredicateDef {
	private final String key;
	private final SortedSet<Object> values;

	public PredicateDef_In(String key, SortedSet<Object> values) {
		this.key = key;
		this.values = values;
	}

	public String getKey() {
		return key;
	}

	public SortedSet<Object> getValues() {
		return values;
	}

	@Override
	public PredicateDef simplify() {
		return (values.iterator().hasNext()) ? this : AggregationPredicates.alwaysFalse();
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
		return isNe(
				value(false),
				call(value(values), "contains",
						cast(property(record, key.replace('.', '$')), Object.class)));
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
