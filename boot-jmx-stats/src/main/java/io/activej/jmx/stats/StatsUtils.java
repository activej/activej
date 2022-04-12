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

package io.activej.jmx.stats;

import io.activej.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static io.activej.common.Utils.first;
import static io.activej.common.reflection.ReflectionUtils.isSimpleType;
import static java.lang.System.identityHashCode;
import static java.util.Arrays.asList;

public final class StatsUtils {
	public static boolean isJmxStats(Class<?> cls) {
		return JmxStats.class.isAssignableFrom(cls);
	}

	public static boolean isJmxRefreshableStats(Class<?> cls) {
		return JmxRefreshableStats.class.isAssignableFrom(cls);
	}

	public static void resetStats(Object instance) {
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithReset) {
				((JmxStatsWithReset) item).resetStats();
				return true;
			}
			return false;
		});
	}

	public static void setSmoothingWindow(Object instance, Duration smoothingWindowSeconds) {
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithSmoothingWindow) {
				((JmxStatsWithSmoothingWindow) item).setSmoothingWindow(smoothingWindowSeconds);
				return true;
			}
			return false;
		});
	}

	public static @Nullable Duration getSmoothingWindow(Object instance) {
		Set<Duration> result = new HashSet<>();
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithSmoothingWindow) {
				Duration smoothingWindow = ((JmxStatsWithSmoothingWindow) item).getSmoothingWindow();
				result.add(smoothingWindow);
				return true;
			}
			return false;
		});
		if (result.size() == 1) {
			return first(result);
		}
		return null;
	}

	private static final ThreadLocal<Set<Object>> VISITED = ThreadLocal.withInitial(HashSet::new);
	private static final ThreadLocal<Integer> RECURSION_LEVEL = ThreadLocal.withInitial(() -> 0);

	private static void visitFields(Object instance, Predicate<Object> action) {
		RECURSION_LEVEL.set(RECURSION_LEVEL.get() + 1);
		try {
			doVisitFields(instance, action);
		} finally {
			Integer oldRecursionLevel = RECURSION_LEVEL.get();
			RECURSION_LEVEL.set(oldRecursionLevel - 1);
			if (oldRecursionLevel == 1) {
				VISITED.get().clear();
			}
		}
	}

	private static void doVisitFields(Object instance, Predicate<Object> action) {
		if (instance == null) return;

		for (Method method : instance.getClass().getMethods()) {
			if (method.getParameters().length != 0) {
				continue;
			}
			Class<?> returnType = method.getReturnType();
			if (returnType == void.class || isSimpleType(returnType)) {
				continue;
			}
			if (!method.isAnnotationPresent(JmxAttribute.class)) {
				continue;
			}

			if (!VISITED.get().add(asList(identityHashCode(instance), method))) {
				continue;
			}

			Object fieldValue;
			try {
				fieldValue = method.invoke(instance);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.getCause());
			}
			if (fieldValue == null) {
				continue;
			}
			if (action.test(fieldValue)) {
				continue;
			}
			if (Map.class.isAssignableFrom(returnType)) {
				for (Object item : ((Map<?, ?>) fieldValue).values()) {
					doVisitFields(item, action);
				}
			} else if (Collection.class.isAssignableFrom(returnType)) {
				for (Object item : (Collection<?>) fieldValue) {
					doVisitFields(item, action);
				}
			} else {
				doVisitFields(fieldValue, action);
			}
		}
	}

}
