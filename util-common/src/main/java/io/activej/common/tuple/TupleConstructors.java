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

import java.util.function.Consumer;
import java.util.function.Supplier;

public final class TupleConstructors {

	public static <T1, R> TupleConstructor1<T1, R> toPojo(Supplier<R> pojoSupplier, Consumer<T1> setter1) {
		return value1 -> {
			R pojo = pojoSupplier.get();
			setter1.accept(value1);
			return pojo;
		};
	}

	public static <T1, T2, R> TupleConstructor2<T1, T2, R> toPojo(Supplier<R> pojoSupplier,
			Consumer<T1> setter1,
			Consumer<T2> setter2) {
		return (value1, value2) -> {
			R pojo = pojoSupplier.get();
			setter1.accept(value1);
			setter2.accept(value2);
			return pojo;
		};
	}

	public static <T1, T2, T3, R> TupleConstructor3<T1, T2, T3, R> toPojo(Supplier<R> pojoSupplier,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3) {
		return (value1, value2, value3) -> {
			R pojo = pojoSupplier.get();
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			return pojo;
		};
	}

	public static <T1, T2, T3, T4, R> TupleConstructor4<T1, T2, T3, T4, R> toPojo(Supplier<R> pojoSupplier,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3,
			Consumer<T4> setter4) {
		return (value1, value2, value3, value4) -> {
			R pojo = pojoSupplier.get();
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			setter4.accept(value4);
			return pojo;
		};
	}

	public static <T1, T2, T3, T4, T5, R> TupleConstructor5<T1, T2, T3, T4, T5, R> toPojo(Supplier<R> pojoSupplier,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3,
			Consumer<T4> setter4,
			Consumer<T5> setter5) {
		return (value1, value2, value3, value4, value5) -> {
			R pojo = pojoSupplier.get();
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			setter4.accept(value4);
			setter5.accept(value5);
			return pojo;
		};
	}

	public static <T1, T2, T3, T4, T5, T6, R> TupleConstructor6<T1, T2, T3, T4, T5, T6, R> toPojo(Supplier<R> pojoSupplier,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3,
			Consumer<T4> setter4,
			Consumer<T5> setter5,
			Consumer<T6> setter6) {
		return (value1, value2, value3, value4, value5, value6) -> {
			R pojo = pojoSupplier.get();
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			setter4.accept(value4);
			setter5.accept(value5);
			setter6.accept(value6);
			return pojo;
		};
	}

	public static <T1, R> TupleConstructor1<T1, R> toPojo(R pojo, Consumer<T1> setter1) {
		return value1 -> {
			setter1.accept(value1);
			return pojo;
		};
	}

	public static <T1, T2, R> TupleConstructor2<T1, T2, R> toPojo(R pojo,
			Consumer<T1> setter1,
			Consumer<T2> setter2) {
		return (value1, value2) -> {
			setter1.accept(value1);
			setter2.accept(value2);
			return pojo;
		};
	}

	public static <T1, T2, T3, R> TupleConstructor3<T1, T2, T3, R> toPojo(R pojo,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3) {
		return (value1, value2, value3) -> {
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			return pojo;
		};
	}

	public static <T1, T2, T3, T4, R> TupleConstructor4<T1, T2, T3, T4, R> toPojo(R pojo,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3,
			Consumer<T4> setter4) {
		return (value1, value2, value3, value4) -> {
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			setter4.accept(value4);
			return pojo;
		};
	}

	public static <T1, T2, T3, T4, T5, R> TupleConstructor5<T1, T2, T3, T4, T5, R> toPojo(R pojo,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3,
			Consumer<T4> setter4,
			Consumer<T5> setter5) {
		return (value1, value2, value3, value4, value5) -> {
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			setter4.accept(value4);
			setter5.accept(value5);
			return pojo;
		};
	}

	public static <T1, T2, T3, T4, T5, T6, R> TupleConstructor6<T1, T2, T3, T4, T5, T6, R> toPojo(R pojo,
			Consumer<T1> setter1,
			Consumer<T2> setter2,
			Consumer<T3> setter3,
			Consumer<T4> setter4,
			Consumer<T5> setter5,
			Consumer<T6> setter6) {
		return (value1, value2, value3, value4, value5, value6) -> {
			setter1.accept(value1);
			setter2.accept(value2);
			setter3.accept(value3);
			setter4.accept(value4);
			setter5.accept(value5);
			setter6.accept(value6);
			return pojo;
		};
	}
}
