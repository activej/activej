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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class AttributeNodeForConverterType<T> extends AbstractAttributeNodeForLeaf {
	private static final Logger logger = LoggerFactory.getLogger(AttributeNodeForConverterType.class);

	private final @Nullable Method setter;
	private final Function<T, String> to;
	private final @Nullable Function<String, T> from;

	public AttributeNodeForConverterType(String name, @Nullable String description, ValueFetcher fetcher,
			boolean visible, @Nullable Method setter,
			Function<T, String> to, @Nullable Function<String, T> from) {
		super(name, description, fetcher, visible);
		this.setter = setter;
		this.to = to;
		this.from = from;
	}

	public AttributeNodeForConverterType(String name, @Nullable String description, boolean visible,
			ValueFetcher fetcher, @Nullable Method setter,
			Function<T, String> to, @Nullable Function<String, T> from) {
		this(name, description, fetcher, visible, setter, to, from);
	}

	public AttributeNodeForConverterType(String name, String description, boolean visible,
			ValueFetcher fetcher, Method setter,
			Function<T, String> to) {
		this(name, description, fetcher, visible, setter, to, null);
	}

	@Override
	protected @Nullable Object aggregateAttribute(String attrName, List<?> sources) {
		Object firstPojo = sources.get(0);
		Object firstValue = fetcher.fetchFrom(firstPojo);
		if (firstValue == null) {
			return null;
		}

		for (int i = 1; i < sources.size(); i++) {
			Object currentPojo = sources.get(i);
			Object currentValue = fetcher.fetchFrom(currentPojo);
			if (!Objects.equals(firstValue, currentValue)) {
				return null;
			}
		}
		return to.apply((T) firstValue);
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		return Map.of(name, SimpleType.STRING);
	}

	@Override
	public List<JmxRefreshable> getAllRefreshables(@NotNull Object source) {
		return List.of();
	}

	@Override
	public boolean isSettable(@NotNull String attrName) {
		return setter != null && from != null;
	}

	@Override
	public void setAttribute(@NotNull String attrName, @NotNull Object value, @NotNull List<?> targets) throws SetterException {
		if (!isSettable("")) {
			throw new SetterException(new IllegalAccessException("Cannot set non writable attribute " + name));
		}
		assert from != null && setter != null; // above settable check
		T result = from.apply((String) value);
		for (Object target : targets) {
			try {
				setter.invoke(target, result);
			} catch (Exception e) {
				logger.error("Can't set attribute " + attrName, e);
			}
		}
	}
}
