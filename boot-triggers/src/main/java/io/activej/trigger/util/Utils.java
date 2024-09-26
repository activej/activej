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

package io.activej.trigger.util;

import io.activej.inject.Key;
import io.activej.types.Types;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

public final class Utils {

	public static String prettyPrintQualifier(Object qualifier) {
		if (qualifier instanceof Annotation) {
			return prettyPrintAnnotation((Annotation) qualifier);
		}
		if (qualifier instanceof Class && ((Class<?>) qualifier).isAnnotation()) {
			return "@" + ((Class<?>) qualifier).getSimpleName();
		}
		return qualifier.toString();
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	public static String prettyPrintAnnotation(Annotation annotation) {
		StringBuilder sb = new StringBuilder();
		Method[] methods = annotation.annotationType().getDeclaredMethods();
		boolean first = true;
		for (Method m : methods) {
			try {
				Object value = m.invoke(annotation);
				if (value.equals(m.getDefaultValue()))
					continue;
				String valueStr = value instanceof String ? "\"" + value + "\"" : value.toString();
				String methodName = m.getName();
				if ("value".equals(methodName) && first) {
					sb.append(valueStr);
				} else {
					sb.append((first ? "" : ",") + methodName + "=" + valueStr);
				}
				first = false;
			} catch (ReflectiveOperationException ignored) {
			}
		}
		String simpleName = annotation.annotationType().getSimpleName();
		return "@" + ("NamedImpl".equals(simpleName) ? "Named" : simpleName) + (first ? "" : "(" + sb + ")");
	}

	public static String prettyPrintSimpleKeyName(Key<?> key) {
		Type type = key.getType();
		return
			(key.getQualifier() != null ?
				prettyPrintQualifier(key.getQualifier()) + " " :
				"") +
			Types.getSimpleName(type);
	}

}
