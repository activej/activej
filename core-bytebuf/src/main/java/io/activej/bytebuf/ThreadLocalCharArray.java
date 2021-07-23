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

package io.activej.bytebuf;

final class ThreadLocalCharArray {
	private ThreadLocalCharArray() {
	}

	private static final ThreadLocal<char[]> THREAD_LOCAL = ThreadLocal.withInitial(() -> new char[0]);

	public static char[] ensure(int size) {
		char[] chars = THREAD_LOCAL.get();
		if (chars.length >= size) return chars;
		chars = new char[size + (size >>> 2)];
		THREAD_LOCAL.set(chars);
		return chars;
	}

	public static char[] ensure(char[] providedBuffer, int size) {
		if (providedBuffer.length >= size) return providedBuffer;
		return ensure(size);
	}
}
