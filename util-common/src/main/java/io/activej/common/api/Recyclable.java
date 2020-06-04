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

import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Since some objects (mainly {@link io.activej.bytebuf.ByteBuf ByteBufs}) have
 * the notion of ownership and recyclability, a way to recognize them is needed.
 * This interface marks objects that need to be recycled at the end of their lifetime,
 * so that some generic abstraction (such as CSP) could try to recycle some object that it consumes.
 */
@SuppressWarnings("JavadocReference")
public interface Recyclable {

	/**
	 * Free some resource that this object possesses, e.g. return itself to the pool etc.
	 */
	void recycle();

	/**
	 * A method that should be called by generic consuming abstractions as described in class doc.
	 */
	static void tryRecycle(@Nullable Object object) {
		if (object instanceof Recyclable) {
			((Recyclable) object).recycle();
		}
	}

	/**
	 * "Recursive implementation of Recyclable for some standard collections".
	 * <p>
	 * This method is called to make sure that some kind of object structure is fully recycled.
	 */
	static void deepRecycle(@Nullable Object object) {
		if (object == null) return;
		if (object instanceof Recyclable) {
			Recyclable recyclable = (Recyclable) object;
			recyclable.recycle();
		} else if (object instanceof Iterator) {
			Iterator<?> it = (Iterator<?>) object;
			while (it.hasNext()) {
				deepRecycle(it.next());
			}
		} else if (object instanceof Collection) {
			deepRecycle(((Collection<?>) object).iterator());
			((Collection<?>) object).clear();
		} else if (object instanceof Iterable) {
			deepRecycle(((Iterable<?>) object).iterator());
		} else if (object instanceof Map) {
			deepRecycle(((Map<?, ?>) object).values());
			((Map<?, ?>) object).clear();
		} else if (object instanceof Object[]) {
			Object[] objects = (Object[]) object;
			for (Object element : objects) {
				deepRecycle(element);
			}
			Arrays.fill(objects, null);
		}
	}
}
