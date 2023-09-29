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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;

/**
 * Represents a query to aggregation. Contains the list of requested keys, fields, predicates and orderings.
 */
final class AggregationQuery {
	private final List<String> keys = new ArrayList<>();
	private final List<String> measures = new ArrayList<>();
	private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();
	private AggregationPredicate precondition = AggregationPredicates.alwaysTrue();

	AggregationQuery() {
	}

	void addKeys(List<String> keys) {
		this.keys.addAll(keys);
	}

	void addMeasures(List<String> fields) {
		this.measures.addAll(fields);
	}

	void setPredicate(AggregationPredicate predicate) {
		this.predicate = predicate;
	}

	void setPrecondition(AggregationPredicate precondition) {
		this.precondition = precondition;
	}

	public List<String> getKeys() {
		return keys;
	}

	public List<String> getMeasures() {
		return unmodifiableList(measures);
	}

	public Set<String> getAllKeys() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		result.addAll(keys);
		result.addAll(predicate.getDimensions());
		return result;
	}

	public AggregationPredicate getPredicate() {
		return predicate;
	}

	public AggregationPredicate getPrecondition() {
		return precondition;
	}

	@Override
	public String toString() {
		return
			"AggregationQuery{" +
			"keys=" + keys +
			", fields=" + measures +
			", predicate=" + predicate +
			", precondition=" + precondition +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AggregationQuery query = (AggregationQuery) o;
		return
			keys.equals(query.keys) &&
			measures.equals(query.measures) &&
			predicate.equals(query.predicate) &&
			precondition.equals(query.precondition);
	}

	@Override
	public int hashCode() {
		int result = keys.hashCode();
		result = 31 * result + measures.hashCode();
		result = 31 * result + predicate.hashCode();
		result = 31 * result + precondition.hashCode();
		return result;
	}
}
