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

import javax.management.openmbean.OpenType;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AttributeNode {
	String getName();

	Set<String> getAllAttributes();

	Set<String> getVisibleAttributes();

	Map<String, Map<String, String>> getDescriptions();

	Map<String, OpenType<?>> getOpenTypes();

	Map<String, Object> aggregateAttributes(Set<String> attrNames, List<?> sources);

	List<JmxRefreshable> getAllRefreshables(Object source);

	boolean isSettable(String attrName);

	void setAttribute(String attrName, Object value, List<?> targets) throws SetterException;

	boolean isVisible();

	void setVisible(String attrName);

	void hideNullPojos(List<?> sources);

	void applyModifier(String attrName, AttributeModifier<?> modifier, List<?> target);
}
