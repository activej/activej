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

package io.activej.serializer;

/**
 * Represents a serializer which encodes and decodes &lt;T&gt; values to byte arrays
 */
public interface BinarySerializer<T> {
	default int encode(byte[] array, int pos, T item) {
		BinaryOutput out = new BinaryOutput(array, pos);
		encode(out, item);
		return out.pos();
	}

	default T decode(byte[] array, int pos) {
		return decode(new BinaryInput(array, pos));
	}

	void encode(BinaryOutput out, T item);

	T decode(BinaryInput in);
}
