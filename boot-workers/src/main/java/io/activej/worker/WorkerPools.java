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

package io.activej.worker;

import io.activej.di.Injector;
import io.activej.di.Scope;
import io.activej.worker.annotation.Worker;

import java.util.ArrayList;
import java.util.List;

public final class WorkerPools {
	private final Injector injector;
	private final List<WorkerPool> workerPools = new ArrayList<>();

	WorkerPools(Injector injector) {
		this.injector = injector;
	}

	public synchronized WorkerPool createPool(int size) {
		return createPool(Scope.of(Worker.class), size);
	}

	public synchronized WorkerPool createPool(Scope scope, int size) {
		WorkerPool workerPool = new WorkerPool(injector, workerPools.size(), scope, size);
		workerPools.add(workerPool);
		return workerPool;
	}

	public List<WorkerPool> getWorkerPools() {
		return new ArrayList<>(workerPools);
	}

	public int size() {
		return workerPools.size();
	}
}
