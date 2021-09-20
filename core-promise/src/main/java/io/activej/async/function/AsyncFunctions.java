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

import io.activej.async.process.AsyncExecutors;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class AsyncFunctions {

	@Contract(pure = true)
	public static @NotNull <T, R> AsyncFunction<T, R> buffer(@NotNull AsyncFunction<T, R> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	public static @NotNull <T, R> AsyncFunction<T, R> buffer(int maxParallelCalls, int maxBufferedCalls, @NotNull AsyncFunction<T, R> asyncFunction) {
		return asyncFunction.withExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls));
	}

}
