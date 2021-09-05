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

package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.Consumer;

import static io.activej.common.exception.Utils.propagateRuntimeException;

@FunctionalInterface
public interface ConsumerEx<T> {
	void accept(T t) throws Exception;

	static <T> ConsumerEx<T> of(Consumer<T> uncheckedFn) {
		return t -> {
			try {
				uncheckedFn.accept(t);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static <T> Consumer<T> uncheckedOf(ConsumerEx<T> checkedFn) {
		return t -> {
			try {
				checkedFn.accept(t);
			} catch (Exception ex) {
				propagateRuntimeException(ex);
				throw UncheckedException.of(ex);
			}
		};
	}
}
