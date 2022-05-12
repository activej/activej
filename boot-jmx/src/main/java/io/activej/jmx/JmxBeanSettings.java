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

import io.activej.common.initializer.WithInitializer;
import io.activej.jmx.DynamicMBeanFactory.JmxCustomTypeAdapter;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.activej.common.Checks.checkArgument;

@SuppressWarnings("UnusedReturnValue")
public final class JmxBeanSettings implements WithInitializer<JmxBeanSettings> {
	private final Set<String> includedOptionals = new HashSet<>();
	private final Map<String, AttributeModifier<?>> modifiers = new HashMap<>();
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes = new HashMap<>();

	private JmxBeanSettings(Set<String> includedOptionals, Map<String, ? extends AttributeModifier<?>> modifiers, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		this.includedOptionals.addAll(includedOptionals);
		this.modifiers.putAll(modifiers);
		this.customTypes.putAll(customTypes);
	}

	public static JmxBeanSettings of(Set<String> includedOptionals,
			Map<String, ? extends AttributeModifier<?>> modifiers,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		return new JmxBeanSettings(includedOptionals, modifiers, customTypes);
	}

	public static JmxBeanSettings create() {
		return new JmxBeanSettings(new HashSet<>(), new HashMap<>(), new HashMap<>());
	}

	public static JmxBeanSettings defaultSettings() {
		return new JmxBeanSettings(Set.of(), Map.of(), Map.of());
	}

	public void merge(JmxBeanSettings otherSettings) {
		includedOptionals.addAll(otherSettings.includedOptionals);
		modifiers.putAll(otherSettings.modifiers);
		customTypes.putAll(otherSettings.customTypes);
	}

	public JmxBeanSettings withIncludedOptional(String attrName) {
		includedOptionals.add(attrName);
		return this;
	}

	public JmxBeanSettings withModifier(String attrName, AttributeModifier<?> modifier) {
		checkArgument(!modifiers.containsKey(attrName), "cannot add two modifiers for one attribute");
		modifiers.put(attrName, modifier);
		return this;
	}

	public JmxBeanSettings withCustomTypes(Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		this.customTypes.putAll(customTypes);
		return this;
	}

	public Set<String> getIncludedOptionals() {
		return includedOptionals;
	}

	public Map<String, ? extends AttributeModifier<?>> getModifiers() {
		return modifiers;
	}

	public Map<Type, JmxCustomTypeAdapter<?>> getCustomTypes() {
		return customTypes;
	}
}
