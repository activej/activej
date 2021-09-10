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
import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.first;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.*;

final class AttributeNodeForMap extends AbstractAttributeNodeForLeaf {
	private static final String KEY_COLUMN_NAME = "> key";
	private static final String VALUE_COLUMN_NAME = "value";
	private static final String EMPTY_COLUMN_NAME = "default";
	private static final String ROW_TYPE_NAME = "RowType";
	private static final String TABULAR_TYPE_NAME = "TabularType";

	private final AttributeNode subNode;
	private final TabularType tabularType;
	private final boolean isMapOfJmxRefreshable;

	public AttributeNodeForMap(String name, @Nullable String description, boolean visible,
			ValueFetcher fetcher, AttributeNode subNode, boolean isMapOfJmxRefreshable) {
		super(name, description, fetcher, visible);
		checkArgument(!name.isEmpty(), "Map attribute cannot have empty name");
		this.tabularType = createTabularType(subNode, name);
		this.subNode = subNode;
		this.isMapOfJmxRefreshable = isMapOfJmxRefreshable;
	}

	private static TabularType createTabularType(AttributeNode subNode, String name) {
		String nodeName = "Attribute name = " + name;

		Set<String> visibleAttrs = subNode.getVisibleAttributes();
		Map<String, OpenType<?>> attrTypes = subNode.getOpenTypes();

		if (visibleAttrs.isEmpty()) {
			throw new IllegalArgumentException("Arrays must have at least one visible attribute. " + nodeName);
		}

		Map<String, OpenType<?>> attrNameToType = new HashMap<>();
		attrNameToType.put(KEY_COLUMN_NAME, SimpleType.STRING);
		if (visibleAttrs.size() == 1) {
			String attrName = first(visibleAttrs);
			OpenType<?> attrType = attrTypes.get(attrName);
			String adjustedAttrName = attrName.isEmpty() ? VALUE_COLUMN_NAME : attrName;
			attrNameToType.put(adjustedAttrName, attrType);
		} else {
			for (String attrName : visibleAttrs) {
				OpenType<?> attrType = attrTypes.get(attrName);
				String adjustedAttrName = attrName.isEmpty() ? EMPTY_COLUMN_NAME : attrName;

				if (attrNameToType.containsKey(adjustedAttrName)) {
					throw new IllegalArgumentException("In case of empty attribute name there must not be another " +
							"empty attribute and attribute with name \"default\"." + nodeName);
				}

				attrNameToType.put(attrName, attrType);
			}
		}

		List<String> columnNames = new ArrayList<>();
		List<OpenType<?>> columnTypes = new ArrayList<>();
		for (Map.Entry<String, OpenType<?>> entry : attrNameToType.entrySet()) {
			columnNames.add(entry.getKey());
			columnTypes.add(entry.getValue());
		}
		String[] columnNamesArr = columnNames.toArray(new String[0]);
		OpenType<?>[] columnTypesArr = columnTypes.toArray(new OpenType<?>[0]);

		try {
			return new TabularType(
					TABULAR_TYPE_NAME,
					TABULAR_TYPE_NAME,
					new CompositeType(ROW_TYPE_NAME, ROW_TYPE_NAME, columnNamesArr, columnNamesArr, columnTypesArr),
					new String[]{KEY_COLUMN_NAME}
			);
		} catch (OpenDataException e) {
			throw new IllegalArgumentException("Cannot create TabularType. " + nodeName, e);
		}
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		return Collections.singletonMap(name, tabularType);
	}

	@Override
	public @Nullable Object aggregateAttribute(String attrName, List<?> sources) {
		Map<Object, List<Object>> groupedByKey = fetchMapsAndGroupEntriesByKey(sources);

		if (groupedByKey.size() == 0) {
			return null;
		}

		TabularDataSupport tdSupport = new TabularDataSupport(tabularType);
		Set<String> visibleSubAttrs = subNode.getVisibleAttributes();
		for (Map.Entry<Object, List<Object>> entry : groupedByKey.entrySet()) {
			Map<String, Object> aggregatedGroup =
					subNode.aggregateAttributes(visibleSubAttrs, entry.getValue());
			try {
				tdSupport.put(createTabularDataRow(entry.getKey().toString(), aggregatedGroup));
			} catch (OpenDataException e) {
				throw new RuntimeException(e);
			}
		}
		return tdSupport.size() > 0 ? tdSupport : null;
	}

	private CompositeData createTabularDataRow(String key, Map<String, Object> attributes) throws OpenDataException {
		Map<String, Object> allAttributes = new HashMap<>(attributes.size() + 1);
		if (attributes.size() == 1) {
			String attrName = first(attributes.keySet());
			Object attrValue = attributes.get(attrName);
			String valueColumnName = attrName.isEmpty() ? VALUE_COLUMN_NAME : attrName;
			allAttributes.put(valueColumnName, attrValue);
		} else {
			if (attributes.containsKey("")) {
				Object emptyColumnValue = attributes.get("");
				attributes.remove("");
				attributes.put(EMPTY_COLUMN_NAME, emptyColumnValue);
			}

			allAttributes.putAll(attributes);
		}
		allAttributes.put(KEY_COLUMN_NAME, key);
		return new CompositeDataSupport(tabularType.getRowType(), allAttributes);
	}

	private Map<Object, List<Object>> fetchMapsAndGroupEntriesByKey(List<?> pojos) {
		List<Map<?, ?>> listOfMaps = new ArrayList<>();
		for (Object pojo : pojos) {
			Map<?, ?> map = (Map<?, ?>) fetcher.fetchFrom(pojo);
			if (map != null && map.size() > 0) {
				listOfMaps.add(new HashMap<>(map));
			}
		}

		return listOfMaps.stream()
				.flatMap(map -> map.entrySet().stream())
				.collect(groupingBy(Map.Entry::getKey, mapping(e -> (Object) e.getValue(), toList())));
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<JmxRefreshable> getAllRefreshables(@NotNull Object source) {
		if (!isMapOfJmxRefreshable) {
			return emptyList();
		}

		Map<?, JmxRefreshable> mapRef = (Map<?, JmxRefreshable>) fetcher.fetchFrom(source);
		return Collections.singletonList(timestamp -> {
			for (JmxRefreshable jmxRefreshableValue : mapRef.values()) {
				jmxRefreshableValue.refresh(timestamp);
			}
		});
	}

	@Override
	public boolean isSettable(@NotNull String attrName) {
		return false;
	}

	@Override
	public void setAttribute(@NotNull String attrName, @NotNull Object value, @NotNull List<?> targets) {
		throw new UnsupportedOperationException("Cannot set attributes for map attribute node");
	}
}
