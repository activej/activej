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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.activej.common.Checks.checkArgument;
import static io.activej.config.Config.concatPath;

public final class Config_WithFullPath implements Config {
	private final String path;
	private final Config config;
	private final Map<String, Config> children;

	private Config_WithFullPath(String path, Config config) {
		this.path = path;
		this.config = config;
		this.children = new LinkedHashMap<>();
		config.getChildren().forEach((key, value) ->
				this.children.put(key, new Config_WithFullPath(concatPath(this.path, key), value)));
	}

	public static Config_WithFullPath wrap(Config config) {
		return new Config_WithFullPath("", config);
	}

	@Override
	public String getValue(String defaultValue) {
		return config.getValue(defaultValue);
	}

	@Override
	public String getValue() throws NoSuchElementException {
		try {
			return config.getValue();
		} catch (NoSuchElementException ignored) {
			throw new NoSuchElementException(path);
		}
	}

	@Override
	public Map<String, Config> getChildren() {
		return children;
	}

	@Override
	public Config provideNoKeyChild(String key) {
		checkArgument(!children.containsKey(key), "Children already contain key '%s'", key);
		return new Config_WithFullPath(concatPath(path, key), config.provideNoKeyChild(key));
	}

}
