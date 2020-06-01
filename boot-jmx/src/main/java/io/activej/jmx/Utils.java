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

import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.JmxBeanAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.management.DynamicMBean;
import javax.management.MXBean;
import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.activej.common.reflection.ReflectionUtils.*;

class Utils {
	static boolean isMBean(Class<?> clazz) {
		return isJmxBean(clazz) || isStandardMBean(clazz) || isMXBean(clazz) || isDynamicMBean(clazz);
	}

	static boolean isJmxBean(Class<?> clazz) {
		return deepFindAnnotation(clazz, JmxBean.class).isPresent();
	}

	static boolean isStandardMBean(Class<?> clazz) {
		return walkClassHierarchy(clazz, anInterface ->
				anInterface.isInterface() && anInterface.getSimpleName().equals(clazz.getSimpleName() + "MBean") ?
						Optional.of(anInterface) :
						Optional.empty())
				.isPresent();
	}

	static boolean isMXBean(Class<?> clazz) {
		return walkClassHierarchy(clazz, anInterface ->
				anInterface.isInterface() && (anInterface.getSimpleName().endsWith("MXBean") || anInterface.isAnnotationPresent(MXBean.class)) ?
						Optional.of(anInterface) :
						Optional.empty())
				.isPresent();
	}

	static boolean isDynamicMBean(Class<?> clazz) {
		return DynamicMBean.class.isAssignableFrom(clazz);
	}

	static Optional<Class<? extends JmxBeanAdapter>> findAdapterClass(Class<?> aClass) {
		return deepFindAnnotation(aClass, JmxBean.class).map(JmxBean::value);
	}

	static String getQualifierString(@NotNull Object qualifier) throws ReflectiveOperationException {
		if (qualifier instanceof Class) {
			Class<?> qualifierClass = (Class<?>) qualifier;
			if (qualifierClass.isAnnotation()) {
				return qualifierClass.getSimpleName();
			}
		} else if (qualifier instanceof Annotation) {
			return getAnnotationString((Annotation) qualifier);
		}
		return qualifier.toString();
	}

	static <T> ProtoObjectNameMapper doMap(Function<@NotNull ProtoObjectName, @Nullable T> extractor,
			UnaryOperator<@NotNull T> mapper,
			BiFunction<@NotNull ProtoObjectName, @Nullable T, @NotNull ProtoObjectName> result) {
		return protoObjectName -> {
			T value = extractor.apply(protoObjectName);
			if (value == null) return protoObjectName;
			T mappedValue = mapper.apply(value);
			return result.apply(protoObjectName, mappedValue);
		};
	}
}
