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

import io.activej.common.initializer.WithInitializer;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;

import java.util.Set;

public final class ChunkLocker_NoOp<C> extends AbstractReactive
		implements AsyncChunkLocker<C>, WithInitializer<ChunkLocker_NoOp<C>> {

	private ChunkLocker_NoOp(Reactor reactor) {
		super(reactor);
	}

	public static <C> ChunkLocker_NoOp<C> create(Reactor reactor) {
		return new ChunkLocker_NoOp<>(reactor);
	}

	@Override
	public Promise<Void> lockChunks(Set<C> chunkIds) {
		checkInReactorThread();
		return Promise.complete();
	}

	@Override
	public Promise<Void> releaseChunks(Set<C> chunkIds) {
		checkInReactorThread();
		return Promise.complete();
	}

	@Override
	public Promise<Set<C>> getLockedChunks() {
		checkInReactorThread();
		return Promise.of(Set.of());
	}
}
