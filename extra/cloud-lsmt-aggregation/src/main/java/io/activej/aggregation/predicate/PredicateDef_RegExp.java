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
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.expression.Variable;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static io.activej.aggregation.predicate.AggregationPredicates.isNotNull;
import static io.activej.codegen.expression.Expressions.*;

public final class PredicateDef_RegExp implements PredicateDef {
	private final String key;
	private final Pattern regexp;

	public PredicateDef_RegExp(String key, Pattern regexp) {
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

	@SuppressWarnings("rawtypes")
	@Override
	public Expression createPredicate(Expression record, Map<String, FieldType> fields) {
		Variable value = property(record, key.replace('.', '$'));
		return Expressions.and(
				isNotNull(value, fields.get(key)),
				isNe(
						value(false),
						call(call(value(regexp), "matcher", toStringValue(fields, key, value)), "matches")));
	}

	@SuppressWarnings("rawtypes")
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
