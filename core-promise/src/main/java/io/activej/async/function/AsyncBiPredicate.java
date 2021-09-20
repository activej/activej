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
import io.activej.common.function.BiConsumerEx;
import io.activej.common.function.BiPredicateEx;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@FunctionalInterface
public interface AsyncBiPredicate<T, U> {
	Promise<Boolean> test(T t, U u);

	/**
	 * Wraps a {@link BiPredicateEx} interface.
	 *
	 * @param predicate a {@link BiPredicateEx}
	 * @return {@link AsyncBiPredicate} that works on top of {@link BiPredicateEx} interface
	 */
	static @NotNull <T, U> AsyncBiPredicate<T, U> of(@NotNull BiPredicateEx<? super T, ? super U> predicate) {
		return (t, u) -> {
			try {
				return Promise.of(predicate.test(t, u));
			} catch (Exception e) {
				handleError(e, predicate);
				return Promise.ofException(e);
			}
		};
	}

	static <T, U> AsyncBiPredicate<T, U> cast(AsyncBiPredicate<T, U> predicate){
		return predicate;
	}

	@Contract(pure = true)
	default @NotNull <R> R transformWith(@NotNull Function<AsyncBiPredicate<T, U>, R> fn) {
		return fn.apply(this);
	}

	@Contract(pure = true)
	default @NotNull AsyncBiPredicate<T, U> async() {
		return (t, u) -> test(t, u).async();
	}

	@Contract(pure = true)
	default @NotNull AsyncBiPredicate<T, U> withExecutor(@NotNull AsyncExecutor asyncExecutor) {
		return (t, u) -> asyncExecutor.execute(() -> test(t, u));
	}

	@Contract(pure = true)
	default @NotNull AsyncBiPredicate<T, U> peek(@NotNull BiConsumerEx<T, U> action) {
		return (t, u) -> {
			try {
				action.accept(t, u);
			} catch (Exception e) {
				handleError(e, action);
				return Promise.ofException(e);
			}
			return test(t, u);
		};
	}
}
