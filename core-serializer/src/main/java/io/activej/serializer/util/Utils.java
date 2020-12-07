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
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public final class Utils {

	@Nullable
	public static <A extends Annotation> A findAnnotation(Class<A> type, Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType() == type)
				return (A) annotation;
		}
		return null;
	}

	public static <T> T of(Supplier<T> supplier) {
		return supplier.get();
	}

	public static String extractRanges(List<Integer> versions) {
		StringBuilder sb = new StringBuilder();
		int size = versions.size();
		int first = 0, second = 0;
		while (first < size) {
			//noinspection StatementWithEmptyBody
			while (++second < size && versions.get(second) - versions.get(second - 1) == 1) {
			}
			if (second - first > 2) {
				sb.append(versions.get(first));
				sb.append('-');
				sb.append(versions.get(second - 1));
				if (second != size) sb.append(',');
				first = second;
			} else {
				for (; first < second; first++) {
					sb.append(versions.get(first));
					if (second != size) sb.append(',');
				}
			}
		}
		return sb.toString();
	}
}
