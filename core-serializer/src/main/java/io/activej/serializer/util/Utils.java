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

package io.activej.serializer.util;

import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "ForLoopReplaceableByForEach"})
public final class Utils {

	public static boolean hasAnnotation(Class<? extends Annotation> type, Annotation[] annotations) {
		return getAnnotation(type, annotations) != null;
	}

	@Nullable
	public static <A extends Annotation> A getAnnotation(Class<A> type, Annotation[] annotations) {
		for (int i = 0; i < annotations.length; i++) {
			if (annotations[i].annotationType() == type) {
				return (A) annotations[i];
			}
		}
		return null;
	}

	public static <A extends Annotation, T> T getAnnotation(Class<A> type, Annotation[] annotations, Function<@Nullable A, T> fn) {
		return fn.apply(getAnnotation(type, annotations));
	}

	public static <T> T eval(Supplier<T> supplier) {
		return supplier.get();
	}
}
