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

package io.activej.datastream.processor.transformer.sort;

import io.activej.common.annotation.ComponentInterface;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;

import java.util.List;

/**
 * This class is for storing partitions of data from the stream during merge sort,
 * it stores data in some external storage to avoid RAM overflow.
 * Data can be stored here with the index of partition and then read to be merged.
 */
@ComponentInterface
public interface IStreamSorterStorage<T> {
	Promise<Integer> newPartitionId();

	/**
	 * Write a partition of elements to the storage.
	 */
	Promise<StreamConsumer<T>> write(int partition);

	/**
	 * Shortcut for {@link #write} that unwraps and returns an actual consumer.
	 */
	default StreamConsumer<T> writeStream(int partition) {
		return StreamConsumers.ofPromise(write(partition));
	}

	/**
	 * Read a partition of elements from the storage.
	 */
	Promise<StreamSupplier<T>> read(int partition);

	/**
	 * Shortcut for {@link #read} that unwraps and returns an actual supplier.
	 */
	default StreamSupplier<T> readStream(int partition) {
		return StreamSuppliers.ofPromise(read(partition));
	}

	/**
	 * Removes listed partitions from the storage.
	 */
	Promise<Void> cleanup(List<Integer> partitionsToDelete);
}
