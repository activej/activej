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

package io.activej.codegen.util;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class Primitives {
	private static final Map<Class<?>, Class<?>> PRIMITIVE;
	private static final Map<Class<?>, Class<?>> WRAPPER;

	static {
		Map<Class<?>, Class<?>> primToWrap = new HashMap<>();
		Map<Class<?>, Class<?>> wrapToPrim = new HashMap<>();

		add(primToWrap, wrapToPrim, Boolean.TYPE, Boolean.class);
		add(primToWrap, wrapToPrim, Byte.TYPE, Byte.class);
		add(primToWrap, wrapToPrim, Character.TYPE, Character.class);
		add(primToWrap, wrapToPrim, Short.TYPE, Short.class);
		add(primToWrap, wrapToPrim, Integer.TYPE, Integer.class);
		add(primToWrap, wrapToPrim, Long.TYPE, Long.class);
		add(primToWrap, wrapToPrim, Float.TYPE, Float.class);
		add(primToWrap, wrapToPrim, Double.TYPE, Double.class);

		PRIMITIVE = Collections.unmodifiableMap(primToWrap);
		WRAPPER = Collections.unmodifiableMap(wrapToPrim);
	}

	private static void add(Map<Class<?>, Class<?>> forward, Map<Class<?>, Class<?>> backward, Class<?> key, Class<?> value) {
		forward.put(key, value);
		backward.put(value, key);
	}

	public static Set<Class<?>> allPrimitiveTypes() {
		return PRIMITIVE.keySet();
	}

	public static boolean isPrimitiveType(@NotNull Type type) {
		//noinspection SuspiciousMethodCalls
		return PRIMITIVE.containsKey(type);
	}

	public static Set<Class<?>> allWrapperTypes() {
		return WRAPPER.keySet();
	}

	public static boolean isWrapperType(@NotNull Type type) {
		//noinspection SuspiciousMethodCalls
		return WRAPPER.containsKey(type);
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> wrap(@NotNull Class<T> type) {
		Class<T> wrapped = (Class<T>) PRIMITIVE.get(type);
		return wrapped == null ? type : wrapped;
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> unwrap(@NotNull Class<T> type) {
		Class<T> unwrapped = (Class<T>) WRAPPER.get(type);
		return unwrapped == null ? type : unwrapped;
	}
}
