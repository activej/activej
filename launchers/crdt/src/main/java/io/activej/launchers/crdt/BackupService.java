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

package io.activej.launchers.crdt;

import io.activej.async.service.EventloopService;
import io.activej.crdt.local.CrdtStorageFs;
import io.activej.crdt.local.CrdtStorageMap;
import io.activej.datastream.StreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class BackupService<K extends Comparable<K>, S> implements EventloopService {
	private final Eventloop eventloop;
	private final CrdtStorageMap<K, S> inMemory;
	private final CrdtStorageFs<K, S> localFiles;

	private long lastTimestamp = 0;

	@Nullable
	private Promise<Void> backupPromise = null;

	public BackupService(CrdtStorageMap<K, S> inMemory, CrdtStorageFs<K, S> localFiles) {
		this.inMemory = inMemory;
		this.localFiles = localFiles;
		this.eventloop = localFiles.getEventloop();
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public Promise<Void> restore() {
		return localFiles.download()
				.then(supplierWithResult ->
						supplierWithResult.streamTo(StreamConsumer.ofPromise(inMemory.upload())));
	}

	public Promise<Void> backup() {
		if (backupPromise != null) {
			return backupPromise;
		}
		long lastTimestamp = this.lastTimestamp;
		this.lastTimestamp = eventloop.currentTimeMillis();
		return backupPromise = inMemory.download(lastTimestamp)
				.then(supplierWithResult -> supplierWithResult
						.streamTo(StreamConsumer.ofPromise(localFiles.upload()))
						.whenComplete(() -> backupPromise = null));
	}

	public boolean backupInProgress() {
		return backupPromise != null;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return restore().then(localFiles::consolidate);
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return backup();
	}
}
