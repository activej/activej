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

package io.activej.multilog;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplierWithResult;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

/**
 * Manages persistence of logs.
 */
public interface AsyncMultilog<T> {
	/**
	 * Creates a {@code StreamConsumer} that persists streamed log items to log.
	 *
	 * @param logPartition log partition name
	 * @return StreamConsumer, which will write records, streamed from wired supplier.
	 */
	Promise<StreamConsumer<T>> write(String logPartition);

	/**
	 * Creates a {@code StreamSupplier} that streams items, contained in a given partition and file, starting at the specified position.
	 *
	 * @param logPartition  name of log partition
	 * @param startLogFile  log file
	 * @param startPosition position
	 * @return StreamSupplier, which will stream read items to its wired consumer.
	 */
	Promise<StreamSupplierWithResult<T, LogPosition>> read(String logPartition,
			LogFile startLogFile, long startPosition,
			@Nullable LogFile endLogFile);

}
