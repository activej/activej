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
import io.activej.codegen.expression.Expressions;
import io.activej.common.annotation.ExposedInternals;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkNotNull;

@ExposedInternals
public final class NotEq implements AggregationPredicate {
	public final String key;
	public final @Nullable Object value;

	public NotEq(String key, @Nullable Object value) {
		this.key = checkNotNull(key);
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
		return Map.of();
	}

	@Override
	public Expression createPredicate(Expression record, ValueResolver valueResolver) {
		Expression property = valueResolver.getProperty(record, key);
		Object arg = valueResolver.transformArg(key, value);
		return arg == null ?
			Expressions.isNotNull(property) :
			or(
				Expressions.isNull(property),
				isNe(property, value(arg))
			);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		NotEq that = (NotEq) o;

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
		return key + "!=" + value;
	}
}
