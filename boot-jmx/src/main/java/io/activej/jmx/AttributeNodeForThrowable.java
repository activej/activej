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

import javax.management.openmbean.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

final class AttributeNodeForThrowable extends AbstractAttributeNodeForLeaf {
	private static final String THROWABLE_TYPE_KEY = "type";
	private static final String THROWABLE_MESSAGE_KEY = "message";
	private static final String THROWABLE_STACK_TRACE_KEY = "stackTrace";

	private final CompositeType compositeType;

	public AttributeNodeForThrowable(String name, @Nullable String description, boolean visible, ValueFetcher fetcher) {
		super(name, description, fetcher, visible);
		this.compositeType = compositeTypeForThrowable();
	}

	private static CompositeType compositeTypeForThrowable() {
		try {
			String[] itemNames = {THROWABLE_TYPE_KEY, THROWABLE_MESSAGE_KEY, THROWABLE_STACK_TRACE_KEY};
			OpenType<?>[] itemTypes = new OpenType<?>[]{
					SimpleType.STRING, SimpleType.STRING, new ArrayType<>(1, SimpleType.STRING)};
			return new CompositeType("CompositeType", "CompositeType", itemNames, itemNames, itemTypes);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		return Collections.singletonMap(name, compositeType);
	}

	@Override
	public @Nullable Object aggregateAttribute(String attrName, List<?> sources) {
		Object firstPojo = sources.get(0);
		//noinspection UnnecessaryLocalVariable
		Throwable firstThrowable = (Throwable) fetcher.fetchFrom(firstPojo);
		Throwable resultThrowable = firstThrowable;
		for (int i = 1; i < sources.size(); i++) {
			Object currentPojo = sources.get(i);
			Throwable currentThrowable = (Throwable) fetcher.fetchFrom(currentPojo);
			if (currentThrowable != null) {
				resultThrowable = currentThrowable;
			}
		}

		Object compositeData;
		try {
			compositeData = createCompositeDataFor(resultThrowable);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
		return compositeData;
	}

	private @Nullable CompositeData createCompositeDataFor(Throwable e) throws OpenDataException {
		if (e == null) {
			return null;
		}
		String type = e.getClass().getName();
		String msg = e.getMessage();
		StringWriter stringWriter = new StringWriter();
		e.printStackTrace(new PrintWriter(stringWriter));
		List<String> stackTrace = asList(stringWriter.toString().split("\n"));
		Map<String, Object> nameToValue = new HashMap<>();
		nameToValue.put(THROWABLE_TYPE_KEY, type);
		nameToValue.put(THROWABLE_MESSAGE_KEY, msg);
		nameToValue.put(THROWABLE_STACK_TRACE_KEY, stackTrace.toArray(new String[0]));
		return new CompositeDataSupport(compositeType, nameToValue);
	}

	@Override
	public List<JmxRefreshable> getAllRefreshables(@NotNull Object source) {
		return emptyList();
	}

	@Override
	public boolean isSettable(@NotNull String attrName) {
		return false;
	}

	@Override
	public void setAttribute(@NotNull String attrName, @NotNull Object value, @NotNull List<?> targets) {
		throw new UnsupportedOperationException("Cannot set attributes for throwable attribute node");
	}
}
