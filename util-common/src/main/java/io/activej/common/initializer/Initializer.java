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

package io.activej.common.initializer;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

@FunctionalInterface
public interface Initializer<T> extends Consumer<T> {

	@SuppressWarnings({"unchecked", "rawtypes"})
	@SafeVarargs
	static <T> Initializer<T> combine(Initializer<? super T>... initializers) {
		return combine((List) asList((Initializer[]) initializers));
	}

	static <T> Initializer<T> combine(Iterable<? extends Initializer<? super T>> initializers) {
		return target -> initializers.forEach(initializer -> initializer.accept(target));
	}
}
