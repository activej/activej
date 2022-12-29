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

package io.activej.ot.system;

import io.activej.common.tuple.*;
import io.activej.ot.TransformResult;
import io.activej.ot.exception.TransformException;

import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public final class MergedOTSystem<D, D1, D2> implements OTSystem<D> {
	private final TupleConstructor2<List<D1>, List<D2>, D> constructor;
	private final Function<D, List<D1>> getter1;
	private final OTSystem<D1> otSystem1;
	private final Function<D, List<D2>> getter2;
	private final OTSystem<D2> otSystem2;

	private MergedOTSystem(TupleConstructor2<List<D1>, List<D2>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2) {
		this.constructor = constructor;
		this.getter1 = getter1;
		this.otSystem1 = otSystem1;
		this.getter2 = getter2;
		this.otSystem2 = otSystem2;
	}

	public static <D, D1, D2> OTSystem<D> mergeOtSystems(TupleConstructor2<List<D1>, List<D2>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2) {
		return new MergedOTSystem<>(constructor, getter1, otSystem1, getter2, otSystem2);
	}

	public static <D, D1, D2, D3> OTSystem<D> mergeOtSystems(TupleConstructor3<List<D1>, List<D2>, List<D3>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2,
			Function<D, List<D3>> getter3, OTSystem<D3> otSystem3) {

		OTSystem<Tuple2<List<D1>, List<D2>>> premerged = mergeOtSystems(Tuple2::new,
				Tuple2::value1, otSystem1,
				Tuple2::value2, otSystem2);

		return mergeOtSystems((tuples, list) -> {
					Tuple2<List<D1>, List<D2>> tuple = extractTuple2(tuples);
					return constructor.create(tuple.value1(), tuple.value2(), list);
				},
				d -> combineLists2(getter1.apply(d), getter2.apply(d), Tuple2::new), premerged,
				getter3, otSystem3);
	}

	public static <D, D1, D2, D3, D4> OTSystem<D> mergeOtSystems(TupleConstructor4<List<D1>, List<D2>, List<D3>, List<D4>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2,
			Function<D, List<D3>> getter3, OTSystem<D3> otSystem3,
			Function<D, List<D4>> getter4, OTSystem<D4> otSystem4) {

		OTSystem<Tuple2<List<D1>, List<D2>>> premerged1 = mergeOtSystems(Tuple2::new,
				Tuple2::value1, otSystem1,
				Tuple2::value2, otSystem2);

		OTSystem<Tuple2<List<D3>, List<D4>>> premerged2 = mergeOtSystems(Tuple2::new,
				Tuple2::value1, otSystem3,
				Tuple2::value2, otSystem4);

		return mergeOtSystems((tuples1, tuples2) -> {
					Tuple2<List<D1>, List<D2>> tuple1 = extractTuple2(tuples1);
					Tuple2<List<D3>, List<D4>> tuple2 = extractTuple2(tuples2);
					return constructor.create(tuple1.value1(), tuple1.value2(), tuple2.value1(), tuple2.value2());
				},
				d -> combineLists2(getter1.apply(d), getter2.apply(d), Tuple2::new), premerged1,
				d -> combineLists2(getter3.apply(d), getter4.apply(d), Tuple2::new), premerged2);
	}

	public static <D, D1, D2, D3, D4, D5> OTSystem<D> mergeOtSystems(TupleConstructor5<List<D1>, List<D2>, List<D3>, List<D4>, List<D5>, D> constructor,
			Function<D, List<D1>> getter1, OTSystem<D1> otSystem1,
			Function<D, List<D2>> getter2, OTSystem<D2> otSystem2,
			Function<D, List<D3>> getter3, OTSystem<D3> otSystem3,
			Function<D, List<D4>> getter4, OTSystem<D4> otSystem4,
			Function<D, List<D5>> getter5, OTSystem<D5> otSystem5) {

		OTSystem<Tuple3<List<D1>, List<D2>, List<D3>>> premerged1 = mergeOtSystems(Tuple3::new,
				Tuple3::value1, otSystem1,
				Tuple3::value2, otSystem2,
				Tuple3::value3, otSystem3);

		OTSystem<Tuple2<List<D4>, List<D5>>> premerged2 = mergeOtSystems(Tuple2::new,
				Tuple2::value1, otSystem4,
				Tuple2::value2, otSystem5);

		return mergeOtSystems((tuples1, tuples2) -> {
					Tuple3<List<D1>, List<D2>, List<D3>> tuple1 = extractTuple3(tuples1);
					Tuple2<List<D4>, List<D5>> tuple2 = extractTuple2(tuples2);
					return constructor.create(tuple1.value1(), tuple1.value2(), tuple1.value3(), tuple2.value1(), tuple2.value2());
				},
				d -> combineLists3(getter1.apply(d), getter2.apply(d), getter3.apply(d), Tuple3::new), premerged1,
				d -> combineLists2(getter4.apply(d), getter5.apply(d), Tuple2::new), premerged2);
	}

	@Override
	public TransformResult<D> transform(List<? extends D> leftDiffs, List<? extends D> rightDiffs) throws
			TransformException {
		List<D1> leftDiffs1 = collect(leftDiffs, getter1);
		List<D2> leftDiffs2 = collect(leftDiffs, getter2);
		List<D1> rightDiffs1 = collect(rightDiffs, getter1);
		List<D2> rightDiffs2 = collect(rightDiffs, getter2);

		TransformResult<D1> transform1 = otSystem1.transform(leftDiffs1, rightDiffs1);
		TransformResult<D2> transform2 = otSystem2.transform(leftDiffs2, rightDiffs2);

		List<D> left = combineLists2(transform1.left, transform2.left, constructor);
		List<D> right = combineLists2(transform1.right, transform2.right, constructor);
		return TransformResult.of(left, right);
	}

	@Override
	public List<D> squash(List<? extends D> ops) {
		if (ops.isEmpty()) {
			return List.of();
		}
		List<D1> squashed1 = otSystem1.squash(collect(ops, getter1));
		List<D2> squashed2 = otSystem2.squash(collect(ops, getter2));

		return combineLists2(squashed1, squashed2, constructor);
	}

	@Override
	public boolean isEmpty(D op) {
		for (D1 operation : getter1.apply(op)) {
			if (!otSystem1.isEmpty(operation)) {
				return false;
			}
		}
		for (D2 operation : getter2.apply(op)) {
			if (!otSystem2.isEmpty(operation)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public <O extends D> List<D> invert(List<O> ops) {
		if (ops.isEmpty()) {
			return List.of();
		}
		List<D1> inverted1 = otSystem1.invert(collect(ops, getter1));
		List<D2> inverted2 = otSystem2.invert(collect(ops, getter2));

		return combineLists2(inverted1, inverted2, constructor);
	}

	private <OP> List<OP> collect(List<? extends D> ops, Function<D, List<OP>> getter) {
		return ops.stream()
				.flatMap(op -> getter.apply(op).stream())
				.collect(toList());
	}

	private static <D, D1, D2> List<D> combineLists2(List<D1> list1, List<D2> list2, TupleConstructor2<List<D1>, List<D2>, D> constructor) {
		return list1.isEmpty() && list2.isEmpty() ?
				List.of() :
				List.of(constructor.create(list1, list2));
	}

	private static <D, D1, D2, D3> List<D> combineLists3(List<D1> list1, List<D2> list2, List<D3> list3, TupleConstructor3<List<D1>, List<D2>, List<D3>, D> constructor) {
		return list1.isEmpty() && list2.isEmpty() && list3.isEmpty() ?
				List.of() :
				List.of(constructor.create(list1, list2, list3));
	}

	private static <D1, D2> Tuple2<List<D1>, List<D2>> extractTuple2(List<Tuple2<List<D1>, List<D2>>> tuples) {
		assert tuples.size() < 2;
		return tuples.isEmpty() ? new Tuple2<>(List.of(), List.of()) : tuples.get(0);
	}

	private static <D1, D2, D3> Tuple3<List<D1>, List<D2>, List<D3>> extractTuple3(List<Tuple3<List<D1>, List<D2>, List<D3>>> tuples) {
		assert tuples.size() < 2;
		return tuples.isEmpty() ? new Tuple3<>(List.of(), List.of(), List.of()) : tuples.get(0);
	}

}
