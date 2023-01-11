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

import io.activej.async.service.ReactiveService;
import io.activej.crdt.storage.local.CrdtStorage_Fs;
import io.activej.crdt.storage.local.CrdtStorage_Map;
import io.activej.datastream.StreamConsumer;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

public final class BackupService<K extends Comparable<K>, S> extends AbstractReactive implements ReactiveService {
	private final CrdtStorage_Map<K, S> inMemory;
	private final CrdtStorage_Fs<K, S> localFiles;

	private long lastTimestamp = 0;

	private @Nullable Promise<Void> backupPromise = null;

	public BackupService(Reactor reactor, CrdtStorage_Map<K, S> inMemory, CrdtStorage_Fs<K, S> localFiles) {
		super(reactor);
		this.inMemory = inMemory;
		this.localFiles = localFiles;
	}

	public Promise<Void> restore() {
		checkInReactorThread();
		return localFiles.download()
				.then(supplierWithResult ->
						supplierWithResult.streamTo(StreamConsumer.ofPromise(inMemory.upload())));
	}

	public Promise<Void> backup() {
		checkInReactorThread();
		if (backupPromise != null) {
			return backupPromise;
		}
		long lastTimestamp = this.lastTimestamp;
		this.lastTimestamp = reactor.currentTimeMillis();
		return backupPromise = inMemory.download(lastTimestamp)
				.then(supplierWithResult -> supplierWithResult
						.streamTo(StreamConsumer.ofPromise(localFiles.upload()))
						.whenComplete(() -> backupPromise = null));
	}

	public boolean backupInProgress() {
		return backupPromise != null;
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread();
		return restore().then(localFiles::consolidate);
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread();
		return backup();
	}
}
