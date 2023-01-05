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

package io.activej.crdt.storage;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtTombstone;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;

/**
 * Interface for various CRDT client implementations.
 * CRDT storage can be seen as SortedMap where put operation is same as merge with CRDT union as combiner.
 *
 * @param <K> type of crdt keys
 * @param <S> type of crdt states
 */
public interface AsyncCrdtStorage<K extends Comparable<K>, S> {

	/**
	 * Returns a promise of a stream consumer of key-state pairs to be added to the CRDT storage.
	 *
	 * @return a promise of a stream consumer of key-state pairs.
	 */
	Promise<StreamConsumer<CrdtData<K, S>>> upload();

	/**
	 * Returns a promise of a stream supplier of all key-state pairs in the CRDT storage that were put AFTER given timestamp was received.
	 * Pairs are sorted by key.
	 *
	 * @return a promise of a stream supplier of key-state pairs
	 */
	Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp);

	/**
	 * Same as above, but downloads all possible key-state pairs.
	 *
	 * @return a promise of a stream consumer of all possible key-state pairs
	 */
	default Promise<StreamSupplier<CrdtData<K, S>>> download() {
		return download(0);
	}

	/**
	 * Returns a promise of a "destroying" stream supplier of all key-state pairs in the CRDT storage.
	 * Pairs are sorted by key. After stream supplier is finished, all the returned key-state pairs
	 * are removed from the CRDT storage.
	 *
	 * @return a promise of a "destroying" stream supplier of all possible key-state pairs
	 */
	Promise<StreamSupplier<CrdtData<K, S>>> take();

	/**
	 * Returns a promise of a stream consumer of tombstones (keyus to be removed from the CRDT storage).
	 * This operation is not persistent and not guaranteed.
	 *
	 * @return a promise of a stream consumer of tombstones
	 */
	Promise<StreamConsumer<CrdtTombstone<K>>> remove();

	/**
	 * Marker that this client is functional (server is up, there are enough nodes in cluster etc.)
	 *
	 * @return promise that succeeds if this client is up
	 */
	Promise<Void> ping();
}
