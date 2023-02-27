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

package io.activej.async.executor;

import io.activej.async.callback.AsyncComputation;
import io.activej.common.function.RunnableEx;
import io.activej.common.function.SupplierEx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * An abstraction over reactor that can receive, dispatch and run tasks in it.
 *
 * @see BlockingReactorExecutor
 */
public interface ReactorExecutor extends Executor {
	/**
	 * Executes the given computation at some time in the future in some underlying reactor.
	 */
	CompletableFuture<Void> submit(RunnableEx computation);

	/**
	 * Executes the given computation at some time in the future in some underlying reactor
	 * and returns its result in a {@link CompletableFuture future}.
	 */
	<T> CompletableFuture<T> submit(AsyncComputation<? extends T> computation);

	/**
	 * Executes the given computation at some time in the future in some underlying reactor
	 * and returns its result in a {@link CompletableFuture future}.
	 */
	default <T> CompletableFuture<T> submit(SupplierEx<? extends AsyncComputation<? extends T>> computation) {
		return submit(AsyncComputation.ofDeferred(computation));
	}
}
