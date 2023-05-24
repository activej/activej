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

package io.activej.trigger.util;

import io.activej.inject.Key;
import io.activej.worker.WorkerPool;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class KeyWithWorkerData {
	private final Key<?> key;

	private final @Nullable WorkerPool pool;
	private final int workerId;

	public KeyWithWorkerData(Key<?> key) {
		this(key, null, -1);
	}

	public KeyWithWorkerData(Key<?> key, @Nullable WorkerPool pool, int workerId) {
		this.key = key;
		this.pool = pool;
		this.workerId = workerId;
	}

	public Key<?> getKey() {
		return key;
	}

	public @Nullable WorkerPool getPool() {
		return pool;
	}

	public int getWorkerId() {
		return workerId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KeyWithWorkerData that = (KeyWithWorkerData) o;

		return workerId == that.workerId
			   && key.equals(that.key)
			   && Objects.equals(pool, that.pool);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * key.hashCode() + (pool != null ? pool.hashCode() : 0)) + workerId;
	}

	@Override
	public String toString() {
		return "KeyWithWorkerData{key=" + key + ", pool=" + pool + ", workerId=" + workerId + '}';
	}
}
