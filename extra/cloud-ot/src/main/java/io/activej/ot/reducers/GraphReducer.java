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

package io.activej.ot.reducers;

import io.activej.ot.OTCommit;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public interface GraphReducer<K, D, R> {
	default void onStart(@NotNull Collection<OTCommit<K, D>> queue) {
	}

	@SuppressWarnings("WeakerAccess")
	final class Result<R> {
		private final boolean resume;
		private final boolean skip;
		private final R value;

		public Result(boolean resume, boolean skip, R value) {
			this.resume = resume;
			this.skip = skip;
			this.value = value;
		}

		public static <T> Result<T> resume() {
			return new Result<>(true, false, null);
		}

		public static <T> Result<T> skip() {
			return new Result<>(false, true, null);
		}

		public static <T> Result<T> complete(T value) {
			return new Result<>(false, false, value);
		}

		public static <T> Promise<Result<T>> resumePromise() {
			return Promise.of(resume());
		}

		public static <T> Promise<Result<T>> skipPromise() {
			return Promise.of(skip());
		}

		public static <T> Promise<Result<T>> completePromise(T value) {
			return Promise.of(complete(value));
		}

		public boolean isResume() {
			return resume;
		}

		public boolean isSkip() {
			return skip;
		}

		public boolean isComplete() {
			return !resume && !skip;
		}

		public R get() {
			if (isComplete()) {
				return value;
			}
			throw new IllegalStateException();
		}
	}

	@NotNull Promise<Result<R>> onCommit(@NotNull OTCommit<K, D> commit);
}
