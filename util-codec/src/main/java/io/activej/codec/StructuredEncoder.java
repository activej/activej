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

package io.activej.codec;

import java.util.List;

/**
 * Encorer can write an object of type T into a {@link StructuredOutput}.
 */
public interface StructuredEncoder<T> {
	void encode(StructuredOutput out, T item);

	static <T> StructuredEncoder<T> ofObject(StructuredEncoder<T> encoder) {
		return (out, item) -> out.writeObject(encoder, item);
	}

	static <T> StructuredEncoder<T> ofObject() {
		return ofObject((out, item) -> {});
	}

	static <T> StructuredEncoder<T> ofTuple(StructuredEncoder<T> encoder) {
		return (out, item) -> out.writeTuple(encoder, item);
	}

	static <T> StructuredEncoder<List<T>> ofList(StructuredEncoder<T> encoder) {
		return (out, list) -> out.writeList(encoder, list);
	}
}
