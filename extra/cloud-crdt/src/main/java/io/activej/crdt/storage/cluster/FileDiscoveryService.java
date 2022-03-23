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

import io.activej.async.function.AsyncSupplier;
import io.activej.common.exception.MalformedDataException;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.types.TypeT;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.activej.crdt.util.Utils.fromJson;
import static java.nio.file.StandardWatchEventKinds.*;

public final class FileDiscoveryService implements DiscoveryService<PartitionId> {
	private static final SettablePromise<PartitionScheme<PartitionId>> UPDATE_HAPPENED = new SettablePromise<>();
	private static final SettablePromise<PartitionScheme<PartitionId>> UPDATE_CONSUMED = new SettablePromise<>();
	private static final TypeT<List<RendezvousPartitionGroup<PartitionId>>> PARTITION_GROUPS_TYPE = new TypeT<List<RendezvousPartitionGroup<PartitionId>>>() {};

	private final Eventloop eventloop;
	private final WatchService watchService;
	private final Path pathToFile;

	private Executor executor = ForkJoinPool.commonPool();

	private FileDiscoveryService(Eventloop eventloop, WatchService watchService, Path pathToFile) {
		this.eventloop = eventloop;
		this.watchService = watchService;
		this.pathToFile = pathToFile;
	}

	public static FileDiscoveryService create(Eventloop eventloop, WatchService watchService, Path pathToFile) throws IOException {
		if (!Files.isRegularFile(pathToFile)) {
			throw new IOException("Not a regular file: " + pathToFile);
		}
		return new FileDiscoveryService(eventloop, watchService, pathToFile);
	}

	public static FileDiscoveryService create(Eventloop eventloop, Path pathToFile) throws IOException {
		WatchService watchService = pathToFile.getFileSystem().newWatchService();
		return create(eventloop, watchService, pathToFile);
	}

	public FileDiscoveryService withExecutor(Executor executor) {
		this.executor = executor;
		return this;
	}

	@Override
	public AsyncSupplier<PartitionScheme<PartitionId>> discover() {
		try {
			pathToFile.getParent().register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
		} catch (IOException e) {
			return () -> Promise.ofException(e);
		}

		return new AsyncSupplier<PartitionScheme<PartitionId>>() {
			final AtomicReference<SettablePromise<PartitionScheme<PartitionId>>> cbRef = new AtomicReference<>(UPDATE_HAPPENED);
			final Thread watchThread;

			{
				watchThread = new Thread(this::watch);
				watchThread.setDaemon(true);
				watchThread.start();
			}

			@Override
			public Promise<PartitionScheme<PartitionId>> get() {
				SettablePromise<PartitionScheme<PartitionId>> cb = cbRef.get();
				if (cb != UPDATE_HAPPENED && cb != UPDATE_CONSUMED) {
					return Promise.ofException(new IOException("Previous completable future has not been completed yet"));
				}

				while (true) {
					if (!watchThread.isAlive()) {
						return Promise.ofException(new IOException("Watch service has been closed"));
					}

					if (cbRef.compareAndSet(UPDATE_HAPPENED, UPDATE_CONSUMED)) {
						return Promise.ofBlocking(executor, () -> {
							byte[] bytes = Files.readAllBytes(pathToFile);
							return parseScheme(bytes);
						});
					}

					SettablePromise<PartitionScheme<PartitionId>> newCb = new SettablePromise<>();
					if (cbRef.compareAndSet(UPDATE_CONSUMED, newCb)) {
						return newCb;
					}
				}
			}

			private void watch() {
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
							onError(new IOException("Watch key is no longer valid"));
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
				if (cbRef.get() == UPDATE_HAPPENED) {
					return;
				}

				if (cbRef.compareAndSet(UPDATE_CONSUMED, UPDATE_HAPPENED)) {
					return;
				}

				SettablePromise<PartitionScheme<PartitionId>> cb = cbRef.getAndSet(UPDATE_CONSUMED);
				assert cb != UPDATE_HAPPENED && cb != UPDATE_CONSUMED;
				try {
					byte[] content = Files.readAllBytes(pathToFile);
					cb.set(parseScheme(content));
				} catch (IOException | MalformedDataException e) {
					onError(e);
				}
			}

			private void onError(Exception e) {
				SettablePromise<PartitionScheme<PartitionId>> cb = cbRef.get();
				if (cb == UPDATE_HAPPENED || cb == UPDATE_CONSUMED) {
					return;
				}

				cbRef.set(UPDATE_CONSUMED);
				eventloop.execute(() -> cb.setException(e));
			}
		};
	}

	private static RendezvousPartitionScheme<PartitionId> parseScheme(byte[] bytes) throws MalformedDataException {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = fromJson(PARTITION_GROUPS_TYPE, bytes);
		return RendezvousPartitionScheme.create(partitionGroups);
	}
}
