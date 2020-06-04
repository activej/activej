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

package io.activej.service;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.activej.common.Preconditions.checkNotNull;
import static io.activej.service.Utils.completedExceptionallyFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

public abstract class AbstractSupplierService<V> implements SupplierService<V> {
	@Nullable
	private V value;

	private final Executor executor;

	public AbstractSupplierService() {
		this(Runnable::run);
	}

	public AbstractSupplierService(Executor executor) {
		this.executor = executor;
	}

	@Override
	public final V get() {
		return checkNotNull(value);
	}

	@Override
	public final CompletableFuture<?> start() {
		CompletableFuture<Object> future = new CompletableFuture<>();
		executor.execute(() -> {
			try {
				this.value = checkNotNull(compute());
				future.complete(null);
			} catch (Exception e) {
				future.completeExceptionally(e);
			}
		});
		return future;
	}

	@NotNull
	protected abstract V compute() throws Exception;

	@Override
	public final CompletableFuture<?> stop() {
		try {
			onStop(value);
			return completedFuture(null);
		} catch (Exception e) {
			return completedExceptionallyFuture(e);
		} finally {
			value = null;
		}
	}

	@SuppressWarnings("WeakerAccess")
	protected void onStop(V value) throws Exception {
	}
}
