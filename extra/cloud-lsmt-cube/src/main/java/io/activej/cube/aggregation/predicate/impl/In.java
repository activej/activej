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

package io.activej.cube.aggregation.predicate.impl;

import io.activej.codegen.expression.Expression;
import io.activej.common.annotation.ExposedInternals;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.aggregation.predicate.AggregationPredicates;

import java.util.*;
import java.util.stream.Collectors;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;

@ExposedInternals
public final class In implements AggregationPredicate {
	public final String key;
	public final SortedSet<Object> values;

	public In(String key, SortedSet<Object> values) {
		this.key = checkNotNull(key);
		this.values = checkArgument(values, v -> v.stream().noneMatch(Objects::isNull));
	}

	@Override
	public AggregationPredicate simplify() {
		return values.iterator().hasNext() ? this : AggregationPredicates.alwaysFalse();
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
	public Expression createPredicate(Expression record, ValueResolver valueResolver) {
		Expression property = valueResolver.getProperty(record, key);
		Set<Object> internalValuesSet = values.stream()
			.map(value -> valueResolver.transformArg(key, value))
			.collect(Collectors.toSet());
		return and(isNotNull(property),
			isNe(value(false),
				call(value(internalValuesSet), "contains",
					cast(property, Object.class))));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		In that = (In) o;

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
		return key + " IN " + joiner;
	}
}
