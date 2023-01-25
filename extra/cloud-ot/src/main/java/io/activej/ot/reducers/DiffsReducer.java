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

package io.activej.ot.reducers;

import io.activej.ot.system.IOTSystem;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static io.activej.common.Utils.concat;

public interface DiffsReducer<A, D> {
	A initialValue();

	A accumulate(A accumulatedDiffs, List<? extends D> diffs);

	A combine(A existing, A additional);

	static <A, D> DiffsReducer<A, D> of(@Nullable A initialValue, BiFunction<A, List<? extends D>, A> reduceFunction) {
		return of(initialValue, reduceFunction, (existing, additional) -> existing);
	}

	static <A, D> DiffsReducer<A, D> of(@Nullable A initialValue, BiFunction<A, List<? extends D>, A> reduceFunction, BinaryOperator<A> combiner) {
		return new DiffsReducer<>() {
			@Override
			public @Nullable A initialValue() {
				return initialValue;
			}

			@Override
			public A accumulate(A accumulatedDiffs, List<? extends D> diffs) {
				return reduceFunction.apply(accumulatedDiffs, diffs);
			}

			@Override
			public A combine(A existing, A additional) {
				return combiner.apply(existing, additional);
			}
		};
	}

	static <D> DiffsReducer<Void, D> toVoid() {
		return of(null, ($, lists) -> null);
	}

	static <D> DiffsReducer<List<D>, D> toList() {
		return of(new ArrayList<>(), (accumulatedDiffs, diffs) ->
				concat(diffs, accumulatedDiffs));
	}

	static <D> DiffsReducer<List<D>, D> toSquashedList(IOTSystem<D> system) {
		return of(new ArrayList<>(), (accumulatedDiffs, diffs) ->
				system.squash(concat(diffs, accumulatedDiffs)));
	}

}
