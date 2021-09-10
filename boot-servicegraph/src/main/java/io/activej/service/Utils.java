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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Utils {

	public static CompletableFuture<Void> combineAll(List<? extends CompletionStage<?>> futures) {
		if (futures.isEmpty()) return CompletableFuture.completedFuture(null);
		CompletableFuture<Void> result = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger(futures.size());
		AtomicReference<Throwable> exception = new AtomicReference<>();
		for (CompletionStage<?> future : futures) {
			future.whenComplete(($, e) -> {
				if (e != null) {
					exception.compareAndSet(null, e);
				}
				if (count.decrementAndGet() == 0) {
					if (exception.get() == null) {
						result.complete(null);
					} else {
						result.completeExceptionally(exception.get());
					}
				}
			});
		}
		return result;
	}

	public static @NotNull <T> CompletableFuture<T> completedExceptionallyFuture(Throwable e) {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(e);
		return future;
	}

}
