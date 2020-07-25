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

package io.activej.config;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.config.Config.concatPath;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

public final class EffectiveConfig implements Config {
	private static final class CallsRegistry {
		final Map<String, String> calls = new HashMap<>();
		final Map<String, String> defaultCalls = new HashMap<>();
		final Map<String, String> all;

		CallsRegistry(Map<String, String> all) {
			this.all = all;
		}

		void registerCall(String path, String value) {
			calls.put(path, value);
		}

		void registerDefaultCall(String path, String value, String defaultValue) {
			calls.put(path, value);
			defaultCalls.put(path, defaultValue);
		}
	}

	private final Config config;
	private final Map<String, Config> children;
	private final String path;
	private final CallsRegistry callsRegistry;

	// creators
	private EffectiveConfig(String path, Config config, CallsRegistry callsRegistry) {
		this.config = config;
		this.path = path;
		this.callsRegistry = callsRegistry;
		this.children = new LinkedHashMap<>();
		for (Map.Entry<String, Config> entry : config.getChildren().entrySet()) {
			this.children.put(entry.getKey(),
					new EffectiveConfig(concatPath(this.path, entry.getKey()), entry.getValue(), this.callsRegistry));
		}
	}

	public static EffectiveConfig wrap(Config config) {
		Map<String, String> allProperties = new LinkedHashMap<>(); // same order as inserted in config
		fetchAllConfigs(config, "", allProperties);
		CallsRegistry callsRegistry = new CallsRegistry(allProperties);
		return new EffectiveConfig("", config, callsRegistry);
	}

	private static void fetchAllConfigs(Config config, String prefix, Map<String, String> container) {
		if (config.isEmpty()) {
			return;
		}
		if (config.hasValue()) {
			container.put(prefix, config.getValue());
			return;
		}
		for (String childName : config.getChildren().keySet()) {
			fetchAllConfigs(config.getChild(childName), concatPath(prefix, childName), container);
		}
	}

	@Override
	public String getValue(@Nullable String defaultValue) {
		String value = config.getValue(defaultValue);
		synchronized (this) {
			callsRegistry.registerCall(path, value);
			if (defaultValue != null) {
				callsRegistry.registerDefaultCall(path, value, defaultValue);
			}
		}
		return value;
	}

	@Override
	public String getValue() {
		String value = config.getValue();
		synchronized (this) {
			callsRegistry.registerCall(path, value);
		}
		return value;
	}

	@Override
	public Map<String, Config> getChildren() {
		return children;
	}

	@Override
	public Config provideNoKeyChild(String key) {
		checkArgument(!children.containsKey(key), "Children already contain key '%s'", key);
		return new EffectiveConfig(concatPath(path, key), config.provideNoKeyChild(key), callsRegistry);
	}

	@Override
	public int hashCode() {
		return config.hashCode();
	}

	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
	@Override
	public boolean equals(Object obj) {
		return config.equals(obj);
	}

	@Override
	public String toString() {
		return config.toString();
	}

	// rendering
	public void saveEffectiveConfigTo(Path outputPath) {
		try {
			String renderedConfig = renderEffectiveConfig();
			Files.write(outputPath, renderedConfig.getBytes(UTF_8), CREATE, WRITE, TRUNCATE_EXISTING);
		} catch (IOException e) {
			throw new RuntimeException("Failed to serialize effective config as properties file", e);
		}
	}

	public synchronized String renderEffectiveConfig() {
		CallsRegistry register = callsRegistry;
		StringBuilder sb = new StringBuilder();

		TreeSet<String> keys = new TreeSet<>();
		keys.addAll(register.calls.keySet());   // to get default
		keys.addAll(register.all.keySet());     // to get unused
		String lastKey = "";
		for (String key : keys) {
			if (!lastKey.isEmpty() && commonDots(lastKey, key) == 0) {
				sb.append("\n");
			}
			lastKey = key;

			String used = register.calls.get(key);
			String value = register.all.get(key);
			String defaultValue = register.defaultCalls.get(key);

			if (!register.calls.containsKey(key)) {
				sb.append("## ");
				writeProperty(sb, key, value);
			} else {
				if (Objects.equals(used, defaultValue)) {
					sb.append("# ");
				}
				writeProperty(sb, key, Objects.toString(used, ""));
			}
		}

		return sb.toString();
	}

	private static int commonDots(String key1, String key2) {
		int commonDots = 0;
		for (int i = 0; i < min(key1.length(), key2.length()); i++) {
			if (key2.charAt(i) != key1.charAt(i)) break;
			if (key2.charAt(i) == '.') commonDots++;
		}
		return commonDots;
	}

	private static void writeProperty(StringBuilder sb, String key, String value) {
		sb.append(encodeForPropertiesFile(key, true));
		sb.append(" = ");
		sb.append(encodeForPropertiesFile(value, false));
		sb.append("\n");
	}

	private static String encodeForPropertiesFile(String string, boolean escapeKey) {
		StringBuilder sb = new StringBuilder(string.length() * 2);

		for (int i = 0; i < string.length(); i++) {
			char c = string.charAt(i);
			if ((c > 61) && (c < 127)) {
				if (c == '\\') {
					sb.append('\\');
					sb.append('\\');
					continue;
				}
				sb.append(c);
				continue;
			}
			switch (c) {
				case ' ':
					if (i == 0 || escapeKey)
						sb.append('\\');
					sb.append(' ');
					break;
				case '\t':
					sb.append('\\');
					sb.append('t');
					break;
				case '\n':
					sb.append('\\');
					sb.append('n');
					break;
				case '\r':
					sb.append('\\');
					sb.append('r');
					break;
				case '\f':
					sb.append('\\');
					sb.append('f');
					break;
				case '=':
				case ':':
					if (escapeKey) sb.append('\\');
					sb.append(c);
					break;
				case '#':
				case '!':
					if (escapeKey && i == 0) sb.append('\\');
					sb.append(c);
					break;
				default:
					sb.append(c);
			}
		}
		return sb.toString();
	}

	public Map<String, String> getEffectiveDefaults() {
		return callsRegistry.defaultCalls;
	}

	public Map<String, String> getEffectiveCalls() {
		return callsRegistry.calls;
	}

}
