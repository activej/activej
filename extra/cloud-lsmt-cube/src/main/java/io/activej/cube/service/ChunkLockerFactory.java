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

package io.activej.cube.service;

import io.activej.aggregation.ChunkLocker;
import io.activej.aggregation.ChunkLockerNoOp;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

class ChunkLockerFactory<C> {
	private static final Logger logger = LoggerFactory.getLogger(ChunkLockerFactory.class);

	public static final ChunkLocker<Object> NOOP_CHUNK_LOCKER = ChunkLockerNoOp.create();

	private final Map<String, DelayedReleaseChunkLocker> lockers = new HashMap<>();

	private final Function<String, ChunkLocker<C>> factory;

	ChunkLockerFactory() {
		//noinspection unchecked
		this.factory = $ -> (ChunkLocker<C>) NOOP_CHUNK_LOCKER;
	}

	ChunkLockerFactory(Function<String, ChunkLocker<C>> factory) {
		this.factory = factory;
	}

	public ChunkLocker<Object> ensureLocker(String aggregationId) {
		//noinspection unchecked
		return lockers.computeIfAbsent(aggregationId,
				$ -> new DelayedReleaseChunkLocker((ChunkLocker<Object>) factory.apply(aggregationId)));
	}

	// Promise never fails
	public Promise<Void> tryReleaseAll() {
		return Promises.all(lockers.entrySet().stream()
				.map(entry -> {
					DelayedReleaseChunkLocker delayedLocker = entry.getValue();
					return delayedLocker.doRelease()
							.thenEx(($, e) -> {
								if (e != null) {
									logger.warn("Failed to release chunks: {} in aggregation {}",
											delayedLocker.chunksToBeReleased,
											entry.getKey());
								}
								return Promise.complete();
							});
				}));
	}

	private static final class DelayedReleaseChunkLocker implements ChunkLocker<Object> {
		private final ChunkLocker<Object> peer;

		private Set<Object> chunksToBeReleased = Collections.emptySet();

		private DelayedReleaseChunkLocker(ChunkLocker<Object> peer) {
			this.peer = peer;
		}

		@Override
		public Promise<Void> lockChunks(Set<Object> chunkIds) {
			return peer.lockChunks(chunkIds);
		}

		@Override
		public Promise<Void> releaseChunks(Set<Object> chunkIds) {
			chunksToBeReleased = chunkIds;
			return Promise.complete();
		}

		private Promise<Void> doRelease() {
			return peer.releaseChunks(chunksToBeReleased);
		}

		@Override
		public Promise<Set<Object>> getLockedChunks() {
			return peer.getLockedChunks();
		}
	}
}
