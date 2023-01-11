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

package io.activej.crdt.storage.cluster;

import com.sun.nio.file.SensitivityWatchEventModifier;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.CrdtException;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.*;

public final class DiscoveryService_File extends AbstractDiscoveryService<DiscoveryService_File> {
	private static final SettablePromise<PartitionScheme<PartitionId>> UPDATE_CONSUMED = new SettablePromise<>();

	private final WatchService watchService;
	private final Path pathToFile;

	private DiscoveryService_File(Reactor reactor, WatchService watchService, Path pathToFile) {
		super(reactor);
		this.watchService = watchService;
		this.pathToFile = pathToFile;
	}

	public static DiscoveryService_File create(Reactor reactor, WatchService watchService, Path pathToFile) throws CrdtException {
		if (!Files.exists(pathToFile)) {
			throw new CrdtException("File does not exist: " + pathToFile);
		}
		if (Files.isDirectory(pathToFile)) {
			throw new CrdtException("File is a directory: " + pathToFile);
		}
		return new DiscoveryService_File(reactor, watchService, pathToFile);
	}

	public static DiscoveryService_File create(Reactor reactor, Path pathToFile) throws CrdtException {
		WatchService watchService;
		try {
			watchService = pathToFile.getFileSystem().newWatchService();
		} catch (IOException e) {
			throw new CrdtException("Could not create a watch service", e);
		}
		return create(reactor, watchService, pathToFile);
	}

	@Override
	public AsyncSupplier<PartitionScheme<PartitionId>> discover() {
		checkInReactorThread();
		try {
			pathToFile.getParent().register(
					watchService,
					new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE},
					SensitivityWatchEventModifier.HIGH
			);
		} catch (IOException e) {
			CrdtException exception = new CrdtException("Could not register a path to the watch service", e);
			return () -> Promise.ofException(exception);
		}

		return new AsyncSupplier<>() {
			final AtomicReference<SettablePromise<PartitionScheme<PartitionId>>> cbRef = new AtomicReference<>(UPDATE_CONSUMED);
			final Thread watchThread;

			{
				watchThread = new Thread(this::watch);
				watchThread.setDaemon(true);
				watchThread.start();
			}

			@Override
			public Promise<PartitionScheme<PartitionId>> get() {
				SettablePromise<PartitionScheme<PartitionId>> cb = cbRef.get();
				if (cb != UPDATE_CONSUMED && !cb.isComplete()) {
					return Promise.ofException(new CrdtException("Previous promise has not been completed yet"));
				}

				while (true) {
					if (!watchThread.isAlive()) {
						return Promise.ofException(new CrdtException("Watch service has been closed"));
					}

					if (cb == UPDATE_CONSUMED) {
						SettablePromise<PartitionScheme<PartitionId>> newCb = new SettablePromise<>();
						if (cbRef.compareAndSet(UPDATE_CONSUMED, newCb)) {
							reactor.startExternalTask();
							return newCb;
						}
						cb = cbRef.get();
						continue;
					}

					return cbRef.getAndSet(UPDATE_CONSUMED);
				}
			}

			private void watch() {
				onChange(); // Initial
				try {
					while (true) {
						WatchKey key = watchService.poll(100, TimeUnit.MILLISECONDS);
						if (key == null) continue;
						for (WatchEvent<?> event : key.pollEvents()) {
							if (pathToFile.equals(pathToFile.resolveSibling(((Path) event.context())))) {
								WatchEvent.Kind<?> kind = event.kind();
								if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
									onChange();
								} else if (kind == ENTRY_DELETE) {
									onError(new FileNotFoundException(pathToFile.toString()));
								}
							}
						}
						if (!key.reset()) {
							onError(new CrdtException("Watch key is no longer valid"));
							return;
						}
					}
				} catch (ClosedWatchServiceException e) {
					onError(e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					onError(e);
				}
			}

			private void onChange() {
				PartitionScheme<PartitionId> partitionScheme;
				try {
					byte[] content = Files.readAllBytes(pathToFile);
					partitionScheme = parseScheme(content);
				} catch (IOException e) {
					onError(new CrdtException("Could not read from file", e));
					return;
				} catch (MalformedDataException e) {
					onError(new CrdtException("Could not parse file content", e));
					return;
				}

				completeCb(cb -> cb.set(partitionScheme));
			}

			private void onError(Exception e) {
				completeCb(cb -> cb.setException(e));
			}

			private void completeCb(Consumer<SettablePromise<PartitionScheme<PartitionId>>> consumer) {
				while (true) {
					SettablePromise<PartitionScheme<PartitionId>> cb = cbRef.get();
					if (cb == UPDATE_CONSUMED || cb.isComplete()) {
						SettablePromise<PartitionScheme<PartitionId>> newCb = new SettablePromise<>();
						consumer.accept(newCb);
						if (cbRef.compareAndSet(cb, newCb)) {
							return;
						}
						continue;
					}

					SettablePromise<PartitionScheme<PartitionId>> prevCb = cbRef.getAndSet(UPDATE_CONSUMED);
					assert !prevCb.isComplete();

					reactor.execute(() -> consumer.accept(prevCb));
					reactor.completeExternalTask();
					return;
				}
			}
		};
	}
}
