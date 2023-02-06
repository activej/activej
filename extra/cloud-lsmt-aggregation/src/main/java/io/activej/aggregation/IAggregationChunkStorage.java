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

package io.activej.aggregation;

import io.activej.aggregation.ot.AggregationStructure;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.annotation.ComponentInterface;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;

import java.util.List;
import java.util.Set;

/**
 * Manages persistence of aggregations (chunks of data).
 */
@ComponentInterface
public interface IAggregationChunkStorage<C> {
	Promise<C> createId();

	/**
	 * Creates a {@code StreamSupplier} that streams records contained in the chunk.
	 * The chunk to read is determined by {@code aggregationId} and {@code id}.
	 *
	 * @param recordClass class of chunk record
	 * @param chunkId     id of chunk
	 * @return StreamSupplier, which will stream read records to its wired consumer.
	 */
	<T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId, DefiningClassLoader classLoader);

	/**
	 * Creates a {@code StreamConsumer} that persists streamed records.
	 * The chunk to write is determined by {@code aggregationId} and {@code id}.
	 *
	 * @param fields      fields of chunk record
	 * @param recordClass class of chunk record
	 * @param chunkId     id of chunk
	 */
	<T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId, DefiningClassLoader classLoader);

	Promise<Void> finish(Set<C> chunkIds);

}


