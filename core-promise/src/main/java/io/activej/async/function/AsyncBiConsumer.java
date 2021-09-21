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

import io.activej.common.function.BiConsumerEx;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@FunctionalInterface
public interface AsyncBiConsumer<T, U> {
	Promise<Void> accept(T t, U u);

	/**
	 * Wraps a {@link BiConsumerEx} interface.
	 *
	 * @param consumer a {@link BiConsumerEx}
	 * @return {@link AsyncBiConsumer} that works on top of {@link BiConsumerEx} interface
	 */
	static <T, U> @NotNull AsyncBiConsumer<T, U> of(@NotNull BiConsumerEx<? super T, ? super U> consumer) {
		return (t, u) -> {
			try {
				consumer.accept(t, u);
			} catch (Exception e) {
				handleError(e, consumer);
				return Promise.ofException(e);
			}
			return Promise.complete();
		};
	}
}
