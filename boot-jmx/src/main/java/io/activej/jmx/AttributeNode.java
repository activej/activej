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

import javax.management.openmbean.OpenType;
import java.util.List;
import java.util.Map;
import java.util.Set;

interface AttributeNode {
	String getName();

	Set<String> getAllAttributes();

	Set<String> getVisibleAttributes();

	Map<String, Map<String, String>> getDescriptions();

	Map<String, OpenType<?>> getOpenTypes();

	Map<String, Object> aggregateAttributes(@NotNull Set<String> attrNames, @NotNull List<?> sources);

	List<JmxRefreshable> getAllRefreshables(@NotNull Object source);

	boolean isSettable(@NotNull String attrName);

	void setAttribute(@NotNull String attrName, @NotNull Object value, @NotNull List<@Nullable ?> targets) throws SetterException;

	boolean isVisible();

	void setVisible(@NotNull String attrName);

	void hideNullPojos(@NotNull List<?> sources);

	void applyModifier(@NotNull String attrName, @NotNull AttributeModifier<?> modifier, @NotNull List<?> target);
}
