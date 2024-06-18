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

package io.activej.cube;

import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.aggregation.predicate.AggregationPredicates;

import java.util.List;
import java.util.Set;

public interface ConsolidationPredicateFactory {
	AggregationPredicate createConsolidationPredicate(String aggregationId, List<String> keys, Set<String> measures);

	static ConsolidationPredicateFactory alwaysTrue() {
		return (aggregationId, keys, measures) -> AggregationPredicates.alwaysTrue();
	}
}
