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

import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.api.attribute.JmxReducer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class AttributeNodeForType<T> extends AbstractAttributeNodeForLeaf {
	private static final Logger logger = LoggerFactory.getLogger(AttributeNodeForType.class);

	private static final Function<String, ?> NOT_CONVERTABLE = $ -> {
		throw new AssertionError();
	};

	private final Class<?> type;
	private final @Nullable Method setter;
	private final JmxReducer reducer;

	private final @Nullable Function<T, String> to;
	private final @Nullable Function<String, T> from;

	public AttributeNodeForType(
		String name, @Nullable String description, ValueFetcher fetcher, boolean visible, @Nullable Method setter,
		Class<?> attributeType, JmxReducer<T> reducer, @Nullable Function<T, String> to,
		@Nullable Function<String, T> from
	) {
		super(name, description, fetcher, visible);
		this.setter = setter;
		this.reducer = reducer;
		this.type = attributeType;
		this.to = to;
		this.from = from;
	}

	public static <T> AttributeNodeForType<T> createCustom(
		String name, @Nullable String description, ValueFetcher fetcher, boolean visible, @Nullable Method setter,
		Function<T, String> to, @Nullable Function<String, T> from, JmxReducer<T> reducer
	) {
		return new AttributeNodeForType<>(name, description, fetcher, visible, setter, String.class, reducer,
			to, from == null ? (Function<String, T>) NOT_CONVERTABLE : from);
	}

	public static <T> AttributeNodeForType<T> createSimple(
		String name, @Nullable String description, ValueFetcher fetcher, boolean visible, @Nullable Method setter,
		Class<?> attributeType, JmxReducer reducer
	) {
		return new AttributeNodeForType<>(name, description, fetcher, visible, setter, attributeType, reducer,
			null, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public @Nullable Object aggregateAttribute(String attrName, List<?> sources) {
		List<Object> values = new ArrayList<>(sources.size());
		for (Object notNullSource : sources) {
			Object currentValue = fetcher.fetchFrom(notNullSource);
			values.add(currentValue);
		}

		Object reduced = reducer.reduce(values);

		if (reduced == null || to == null) {
			return reduced;
		}

		return to.apply((T) reduced);
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		return Map.of(name, simpleTypeOf(type));
	}

	@Override
	public List<JmxRefreshable> getAllRefreshables(Object source) {
		return List.of();
	}

	@Override
	public boolean isSettable(String attrName) {
		checkArgument(attrName.equals(name), "Attribute names do not match");
		return setter != null && from != NOT_CONVERTABLE;
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) throws SetterException {
		if (!isSettable(attrName)) {
			throw new SetterException(new IllegalAccessException("Cannot set non writable attribute " + name));
		}
		Object result = from != null ?
			from.apply((String) value)
			: value;

		assert setter != null; // above settable check
		for (Object target : targets.stream().filter(Objects::nonNull).toList()) {
			try {
				setter.invoke(target, result);
			} catch (Exception e) {
				logger.warn("Can't set attribute {}", attrName, e);
				throw new SetterException(e);
			}
		}
	}

	private static SimpleType<?> simpleTypeOf(Class<?> clazz) throws IllegalArgumentException {
		if (clazz == boolean.class || clazz == Boolean.class) {
			return SimpleType.BOOLEAN;
		} else if (clazz == byte.class || clazz == Byte.class) {
			return SimpleType.BYTE;
		} else if (clazz == short.class || clazz == Short.class) {
			return SimpleType.SHORT;
		} else if (clazz == char.class || clazz == Character.class) {
			return SimpleType.CHARACTER;
		} else if (clazz == int.class || clazz == Integer.class) {
			return SimpleType.INTEGER;
		} else if (clazz == long.class || clazz == Long.class) {
			return SimpleType.LONG;
		} else if (clazz == float.class || clazz == Float.class) {
			return SimpleType.FLOAT;
		} else if (clazz == double.class || clazz == Double.class) {
			return SimpleType.DOUBLE;
		} else if (clazz == String.class) {
			return SimpleType.STRING;
		} else {
			throw new IllegalArgumentException("There is no SimpleType for " + clazz.getName());
		}
	}
}
