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

public final class Tuple4<T1, T2, T3, T4> {
	private final T1 value1;
	private final T2 value2;
	private final T3 value3;
	private final T4 value4;

	public Tuple4(T1 value1, T2 value2, T3 value3, T4 value4) {
		this.value1 = value1;
		this.value2 = value2;
		this.value3 = value3;
		this.value4 = value4;
	}

	@Contract(pure = true)
	public T1 getValue1() {
		return value1;
	}

	@Contract(pure = true)
	public T2 getValue2() {
		return value2;
	}

	@Contract(pure = true)
	public T3 getValue3() {
		return value3;
	}

	@Contract(pure = true)
	public T4 getValue4() {
		return value4;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Tuple4<?, ?, ?, ?> that = (Tuple4<?, ?, ?, ?>) o;
		return Objects.equals(value1, that.value1) &&
				Objects.equals(value2, that.value2) &&
				Objects.equals(value3, that.value3) &&
				Objects.equals(value4, that.value4);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value1, value2, value3, value4);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", "{", "}")
				.add("" + value1)
				.add("" + value2)
				.add("" + value3)
				.add("" + value4)
				.toString();
	}
}
