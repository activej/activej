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

package io.activej.common.tuple;

import org.jetbrains.annotations.Contract;

import java.util.Objects;
import java.util.StringJoiner;

public final class Tuple1<T1> {
	private final T1 value1;

	public Tuple1(T1 value1) {
		this.value1 = value1;
	}

	@Contract(pure = true)
	public T1 getValue1() {
		return value1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Tuple1<?> that = (Tuple1<?>) o;
		return Objects.equals(value1, that.value1);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value1);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", "{", "}")
				.add("" + value1)
				.toString();
	}
}
