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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.management.openmbean.OpenType;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

@SuppressWarnings("rawtypes")
final class AttributeNodeForPojo implements AttributeNode {
	private static final char ATTRIBUTE_NAME_SEPARATOR = '_';

	private final String name;
	private final String description;
	private final ValueFetcher fetcher;
	private final JmxReducer reducer;
	private final Map<String, AttributeNode> fullNameToNode;
	private final List<? extends AttributeNode> subNodes;
	private boolean visible;

	public AttributeNodeForPojo(String name, @Nullable String description, boolean visible,
			ValueFetcher fetcher, @Nullable JmxReducer reducer,
			List<? extends AttributeNode> subNodes) {
		this.name = name;
		this.description = description;
		this.visible = visible;
		this.fetcher = fetcher;
		this.reducer = reducer;
		this.fullNameToNode = createFullNameToNodeMapping(name, subNodes);
		this.subNodes = subNodes;
	}

	private static Map<String, AttributeNode> createFullNameToNodeMapping(String name,
			List<? extends AttributeNode> subNodes) {
		Map<String, AttributeNode> fullNameToNodeMapping = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Set<String> currentSubAttrNames = subNode.getAllAttributes();
			for (String currentSubAttrName : currentSubAttrNames) {
				String currentAttrFullName = addPrefix(currentSubAttrName, name);
				if (fullNameToNodeMapping.containsKey(currentAttrFullName)) {
					throw new IllegalArgumentException(
							"There are several attributes with same name: " + currentSubAttrName);
				}
				fullNameToNodeMapping.put(currentAttrFullName, subNode);
			}
		}
		return fullNameToNodeMapping;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Set<String> getAllAttributes() {
		return fullNameToNode.keySet();
	}

	@Override
	public Set<String> getVisibleAttributes() {
		if (!visible) {
			return Collections.emptySet();
		} else {
			Set<String> allVisibleAttrs = new HashSet<>();
			for (AttributeNode subNode : subNodes) {
				Set<String> visibleSubAttrs = subNode.getVisibleAttributes();
				for (String visibleSubAttr : visibleSubAttrs) {
					String visibleAttr = addPrefix(visibleSubAttr);
					allVisibleAttrs.add(visibleAttr);
				}
			}
			return allVisibleAttrs;
		}
	}

	@Override
	public Map<String, Map<String, String>> getDescriptions() {
		Map<String, Map<String, String>> nameToDescriptions = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Map<String, Map<String, String>> currentSubNodeDescriptions = subNode.getDescriptions();
			for (Map.Entry<String, Map<String, String>> entry : currentSubNodeDescriptions.entrySet()) {
				String resultAttrName = addPrefix(entry.getKey());
				Map<String, String> curDescriptions = new LinkedHashMap<>();
				if (description != null) {
					curDescriptions.put(name, description);
				}
				curDescriptions.putAll(entry.getValue());
				nameToDescriptions.put(resultAttrName, curDescriptions);
			}
		}
		return nameToDescriptions;
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		Map<String, OpenType<?>> allTypes = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> subAttrTypes = subNode.getOpenTypes();
			for (Map.Entry<String, OpenType<?>> entry : subAttrTypes.entrySet()) {
				String attrName = addPrefix(entry.getKey());
				allTypes.put(attrName, entry.getValue());
			}
		}
		return allTypes;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> aggregateAttributes(@NotNull Set<String> attrNames, @NotNull List<?> sources) {
		List<?> notNullSources = sources.stream().filter(Objects::nonNull).collect(Collectors.toList());
		if (notNullSources.isEmpty() || attrNames.isEmpty()) {
			Map<String, Object> nullMap = new HashMap<>();
			for (String attrName : attrNames) {
				nullMap.put(attrName, null);
			}
			return nullMap;
		}

		Map<AttributeNode, Set<String>> groupedAttrs = groupBySubnode(attrNames);

		List<Object> subsources;
		if (notNullSources.size() == 1 || reducer == null) {
			subsources = fetchInnerPojos(notNullSources);
		} else {
			Object reduced = reducer.reduce(fetchInnerPojos(sources));
			subsources = singletonList(reduced);
		}

		Map<String, Object> aggregatedAttrs = new HashMap<>();
		for (Map.Entry<AttributeNode, Set<String>> entry : groupedAttrs.entrySet()) {
			Map<String, Object> subAttrs;

			try {
				subAttrs = entry.getKey().aggregateAttributes(entry.getValue(), subsources);
			} catch (Exception | AssertionError e) {
				for (String subAttrName : entry.getValue()) {
					aggregatedAttrs.put(addPrefix(subAttrName), e);
				}
				continue;
			}

			for (Map.Entry<String, Object> subAttrsEntry : subAttrs.entrySet()) {
				aggregatedAttrs.put(addPrefix(subAttrsEntry.getKey()), subAttrsEntry.getValue());
			}
		}

		return aggregatedAttrs;
	}

	private List<Object> fetchInnerPojos(List<?> outerPojos) {
		List<Object> innerPojos = new ArrayList<>(outerPojos.size());
		for (Object outerPojo : outerPojos) {
			Object pojo = fetcher.fetchFrom(outerPojo);
			if (pojo != null) {
				innerPojos.add(pojo);
			}
		}
		return innerPojos;
	}

	private Map<AttributeNode, Set<String>> groupBySubnode(Set<String> attrNames) {
		Map<AttributeNode, Set<String>> selectedSubnodes = new HashMap<>();
		for (String attrName : attrNames) {
			AttributeNode subnode = fullNameToNode.get(attrName);
			Set<String> subnodeAttrs = selectedSubnodes.computeIfAbsent(subnode, k -> new HashSet<>());
			String adjustedName = removePrefix(attrName);
			subnodeAttrs.add(adjustedName);
		}
		return selectedSubnodes;
	}

	private String removePrefix(String attrName) {
		String adjustedName;
		if (name.isEmpty()) {
			adjustedName = attrName;
		} else {
			adjustedName = attrName.substring(name.length());
			if (adjustedName.length() > 0) {
				adjustedName = adjustedName.substring(1); // remove ATTRIBUTE_NAME_SEPARATOR
			}
		}
		return adjustedName;
	}

	private String addPrefix(String attrName) {
		return addPrefix(attrName, name);
	}

	private static String addPrefix(String attrName, String prefix) {
		if (attrName.isEmpty()) {
			return prefix;
		}

		String actualPrefix = prefix.isEmpty() ? "" : prefix + ATTRIBUTE_NAME_SEPARATOR;
		return actualPrefix + attrName;
	}

	@Override
	public List<JmxRefreshable> getAllRefreshables(@NotNull Object source) {
		Object pojo = fetcher.fetchFrom(source);

		if (pojo == null) {
			return Collections.emptyList();
		}

		if (pojo instanceof JmxRefreshable) {
			return singletonList((JmxRefreshable) pojo);
		}
		List<JmxRefreshable> allJmxRefreshables = new ArrayList<>();
		for (AttributeNode attributeNode : subNodes) {
			List<JmxRefreshable> subNodeRefreshables = attributeNode.getAllRefreshables(pojo);
			allJmxRefreshables.addAll(subNodeRefreshables);
		}
		return allJmxRefreshables;
	}

	@Override
	public boolean isSettable(@NotNull String attrName) {
		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);
		return appropriateSubNode.isSettable(removePrefix(attrName));
	}

	@Override
	public void setAttribute(@NotNull String attrName, @NotNull Object value, @NotNull List<?> targets) throws SetterException {
		List<?> notNullTargets = targets.stream().filter(Objects::nonNull).collect(Collectors.toList());
		if (notNullTargets.isEmpty()) {
			return;
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);
		appropriateSubNode.setAttribute(removePrefix(attrName), value, fetchInnerPojos(targets));
	}

	@Override
	public boolean isVisible() {
		return visible;
	}

	@Override
	public void setVisible(@NotNull String attrName) {
		if (attrName.equals(name)) {
			this.visible = true;
			return;
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);
		appropriateSubNode.setVisible(removePrefix(attrName));
	}

	@Override
	public void hideNullPojos(@NotNull List<?> sources) {
		List<?> innerPojos = fetchInnerPojos(sources);
		if (innerPojos.isEmpty()) {
			this.visible = false;
			return;
		}

		for (AttributeNode subNode : subNodes) {
			subNode.hideNullPojos(innerPojos);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void applyModifier(@NotNull String attrName, @NotNull AttributeModifier<?> modifier, @NotNull List<?> target) {
		if (attrName.equals(name)) {
			AttributeModifier<Object> attrModifierObject = (AttributeModifier<Object>) modifier;
			List<Object> attributes = fetchInnerPojos(target);
			for (Object attribute : attributes) {
				attrModifierObject.apply(attribute);
			}
			return;
		}

		for (Map.Entry<String, AttributeNode> entry : fullNameToNode.entrySet()) {
			if (flattenedAttrNameContainsNode(entry.getKey(), attrName)) {
				entry.getValue().applyModifier(removePrefix(attrName), modifier, fetchInnerPojos(target));
				return;
			}
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}
	}

	private static boolean flattenedAttrNameContainsNode(String flattenedAttrName, String nodeName) {
		return flattenedAttrName.startsWith(nodeName) &&
				(flattenedAttrName.length() == nodeName.length() ||
						flattenedAttrName.charAt(nodeName.length()) == ATTRIBUTE_NAME_SEPARATOR);

	}
}
