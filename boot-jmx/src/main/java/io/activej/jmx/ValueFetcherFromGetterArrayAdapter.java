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

package io.activej.jmx;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

final class ValueFetcherFromGetterArrayAdapter implements ValueFetcher {
	private final Method getter;

	public ValueFetcherFromGetterArrayAdapter(@NotNull Method getter) {
		this.getter = getter;
	}

	@Override
	public Object fetchFrom(Object source) {
		try {
			Object arr = getter.invoke(source);
			if (arr.getClass().getComponentType().isPrimitive()) {
				return wrapPrimitives(arr);
			}
			return List.of((Object[]) arr);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	private List<Object> wrapPrimitives(Object arr) {
		int length = Array.getLength(arr);
		if (length == 0) {
			return List.of();
		}

		List<Object> wrappers = new ArrayList<>(length);
		for (int i = 0; i < length; i++) {
			wrappers.add(Array.get(arr, i));
		}
		return wrappers;
	}
}
