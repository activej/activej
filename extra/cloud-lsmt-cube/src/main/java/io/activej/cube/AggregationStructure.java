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

import io.activej.cube.aggregation.ChunkIdJsonCodec;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.aggregation.predicate.AggregationPredicates;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;

@SuppressWarnings("rawtypes")
public final class AggregationStructure {
	private final ChunkIdJsonCodec<?> chunkIdJsonCodec;
	private final Map<String, FieldType> keyTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> measureTypes = new LinkedHashMap<>();
	private final List<String> partitioningKey = new ArrayList<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();

	private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();
	private AggregationPredicate precondition = AggregationPredicates.alwaysTrue();

	AggregationStructure(ChunkIdJsonCodec<?> chunkIdJsonCodec) {
		this.chunkIdJsonCodec = chunkIdJsonCodec;
	}

	void addKey(String keyId, FieldType type) {
		checkArgument(!keyTypes.containsKey(keyId), "Key '%s' has already been added", keyId);
		keyTypes.put(keyId, type);
	}

	void addMeasure(String measureId, Measure aggregateFunction) {
		checkArgument(!measureTypes.containsKey(measureId), "Measure '%s' has already been added", measureId);
		measureTypes.put(measureId, aggregateFunction.getFieldType());
		measures.put(measureId, aggregateFunction);
	}

	void addIgnoredMeasure(String measureId, FieldType measureType) {
		checkArgument(!measureTypes.containsKey(measureId), "Measure '%s' has already been added", measureId);
		measureTypes.put(measureId, measureType);
	}

	void addPartitioningKey(List<String> partitioningKey) {
		this.partitioningKey.addAll(partitioningKey);
	}

	void setPredicate(AggregationPredicate predicate) {
		this.predicate = predicate;
	}

	void setPrecondition(AggregationPredicate precondition) {
		this.precondition = precondition;
	}

	public ChunkIdJsonCodec<?> getChunkIdJsonCodec() {
		return chunkIdJsonCodec;
	}

	public List<String> getKeys() {
		return new ArrayList<>(keyTypes.keySet());
	}

	public List<String> getMeasures() {
		return new ArrayList<>(measures.keySet());
	}

	public Map<String, FieldType> getKeyTypes() {
		return keyTypes;
	}

	public Map<String, FieldType> getMeasureTypes() {
		return measureTypes;
	}

	public Measure getMeasure(String field) {
		return measures.get(field);
	}

	public FieldType getKeyType(String key) {
		return keyTypes.get(key);
	}

	public FieldType getMeasureType(String field) {
		return measureTypes.get(field);
	}

	public List<String> getPartitioningKey() {
		return partitioningKey;
	}

	public AggregationPredicate getPredicate() {
		return predicate;
	}

	public AggregationPredicate getPrecondition() {
		return precondition;
	}

	public List<String> findFields(List<String> measures) {
		return getMeasures().stream().filter(measures::contains).toList();
	}

}
