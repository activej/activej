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

package io.activej.common.api;

import io.activej.common.recycle.Recyclable;

/**
 * Some objects (mainly {@link io.activej.bytebuf.ByteBuf ByteBufs}) have the reference counter
 * so that they can be 'lightly cloned' or 'sliced' and the {@link Recyclable#recycle() recycle} method should be called
 * on all 'light clones' or 'slices' for the object to be actually recycled.
 * This is used to share the ownership between multiple consumers.
 */
@SuppressWarnings("JavadocReference")
public interface Sliceable<T> {
	/**
	 * Creates a 'light clone' of this object.
	 * <p>
	 * This can return either 'this' with reference counter increased.
	 * Or a new wrapper around something that has its reference counter increased.
	 */
	T slice();

	/**
	 * If a given object is sliceable, return a slice, or else just return the object.
	 */
	@SuppressWarnings("unchecked")
	static <T> T trySlice(T value) {
		if (value instanceof Sliceable) return ((Sliceable<T>) value).slice();
		return value;
	}
}
