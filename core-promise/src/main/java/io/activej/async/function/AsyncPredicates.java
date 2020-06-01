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

import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.function.Predicate;

public class AsyncPredicates {

	public static class AsyncPredicateWrapper<T> implements AsyncPredicate<T> {
		@NotNull
		private final Predicate<T> predicate;

		public AsyncPredicateWrapper(@NotNull Predicate<T> predicate) {this.predicate = predicate;}

		@Override
		public Promise<Boolean> test(T t) {
			return Promise.of(predicate.test(t));
		}

		@NotNull
		public Predicate<T> getPredicate() {
			return predicate;
		}
	}

}
