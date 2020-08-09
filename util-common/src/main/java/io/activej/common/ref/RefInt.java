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

package io.activej.common.ref;

public final class RefInt {
	public int value;

	public RefInt(int value) {
		this.value = value;
	}

	public int inc() {
		return ++value;
	}

	public int dec() {
		return --value;
	}

	public int add(int add) {
		return value += add;
	}

	public int get() {
		return value;
	}

	public void set(int peer) {
		this.value = peer;
	}

	@Override
	public String toString() {
		return "→" + value;
	}
}

