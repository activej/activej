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

import java.util.Map;
import java.util.Set;

import static io.activej.codegen.expression.Expressions.value;

@ExposedInternals
public final class AlwaysFalse implements AggregationPredicate {
	public static final AlwaysFalse INSTANCE = new AlwaysFalse();

	@Override
	public AggregationPredicate simplify() {
		return this;
	}

	@Override
	public Set<String> getDimensions() {
		return Set.of();
	}

	@Override
	public Map<String, Object> getFullySpecifiedDimensions() {
		return Map.of();
	}

	@Override
	public Expression createPredicate(Expression record, ValueResolver valueResolver) {
		return value(false);
	}

	@Override
	public String toString() {
		return "FALSE";
	}
}
