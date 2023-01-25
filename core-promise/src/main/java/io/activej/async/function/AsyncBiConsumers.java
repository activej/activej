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

package io.activej.async.function;

import io.activej.async.process.AsyncExecutor;
import io.activej.async.process.AsyncExecutors;
import org.jetbrains.annotations.Contract;

public final class AsyncBiConsumers {

	@Contract(pure = true)
	public static <T, U> AsyncBiConsumer<T, U> buffer(AsyncBiConsumer<T, U> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	public static <T, U> AsyncBiConsumer<T, U> buffer(int maxParallelCalls, int maxBufferedCalls, AsyncBiConsumer<T, U> consumer) {
		return ofExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls), consumer);
	}

	@Contract(pure = true)
	public static <T, U> AsyncBiConsumer<T, U> ofExecutor(AsyncExecutor executor, AsyncBiConsumer<T, U> consumer) {
		return (t, u) -> executor.execute(() -> consumer.accept(t, u));
	}
}
