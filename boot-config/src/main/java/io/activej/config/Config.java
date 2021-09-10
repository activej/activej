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

import io.activej.config.converter.ConfigConverter;
import io.activej.config.converter.ConfigConverters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nonNullElse;
import static io.activej.common.Utils.nonNullOrException;
import static java.util.Collections.*;

/**
 * Interface for interaction with configs.
 */
public interface Config {
	Logger logger = LoggerFactory.getLogger(Config.class);

	/**
	 * Empty config with no values, children, etc...
	 */
	Config EMPTY = new Config() {
		@Override
		public @Nullable String getValue(@Nullable String defaultValue) {
			return defaultValue;
		}

		@Override
		public String getValue() throws NoSuchElementException {
			throw new NoSuchElementException("No value at empty config node");
		}

		@Override
		public Map<String, Config> getChildren() {
			return emptyMap();
		}
	};

	String THIS = "";
	String DELIMITER = ".";
	Pattern DELIMITER_PATTERN = Pattern.compile(Pattern.quote(DELIMITER));
	Pattern PATH_PATTERN = Pattern.compile("([0-9a-zA-Z_-]+(\\.[0-9a-zA-Z_-]+)*)?");

	static String concatPath(String prefix, String suffix) {
		return prefix.isEmpty() || suffix.isEmpty() ? prefix + suffix : prefix + DELIMITER + suffix;
	}

	static void checkPath(String path) {
		checkArgument(PATH_PATTERN.matcher(path).matches(), "Invalid path %s", path);
	}

	/**
	 * @return value stored in root or defaultValue
	 */
	default String getValue(@Nullable String defaultValue) {
		return get(THIS, defaultValue);
	}

	/**
	 * @return value stored in root
	 * @throws NoSuchElementException if there is nothing in root
	 */
	default String getValue() throws NoSuchElementException {
		return get(THIS);
	}

	Map<String, Config> getChildren();

	default boolean hasValue() {
		return getValue(null) != null;
	}

	default boolean hasChildren() {
		return !getChildren().isEmpty();
	}

	default boolean hasChild(String path) {
		checkPath(path);
		Config config = this;
		for (String key : DELIMITER_PATTERN.split(path)) {
			if (key.isEmpty()) {
				continue;
			}
			Map<String, Config> children = config.getChildren();
			if (!children.containsKey(key)) {
				return false;
			}
			config = children.get(key);
		}
		return true;
	}

	default boolean isEmpty() {
		return !hasValue() && !hasChildren();
	}

	/**
	 * Throw {@code NoSuchElementException} if there is no value in path.
	 *
	 * @return String value that lays in path
	 * @see Config#get(ConfigConverter, String, Object)
	 */
	default String get(String path) throws NoSuchElementException {
		checkPath(path);
		return getChild(path).getValue();
	}

	/**
	 * @return String value that lays in path
	 * @see Config#get(ConfigConverter, String, Object)
	 */
	default String get(String path, @Nullable String defaultValue) {
		checkPath(path);
		return getChild(path).getValue(defaultValue);
	}

	/**
	 * Throw {@code NoSuchElementException} if there is no value in path.
	 *
	 * @return value converted to &lt;T&gt;
	 * @see Config#get(ConfigConverter, String, Object)
	 */
	default <T> T get(ConfigConverter<T> converter, String path) throws NoSuchElementException {
		return converter.get(getChild(path));
	}

	/**
	 * @param converter specifies how to convert config string value into &lt;T&gt;
	 * @param path      path to config value. Example: "rpc.server.port" to get port for rpc server.
	 * @param <T>       return type
	 * @return value from this {@code Config} in path or defaultValue if there is nothing on that path
	 * @see ConfigConverters
	 */
	default <T> T get(ConfigConverter<T> converter, String path, @Nullable T defaultValue) {
		return converter.get(getChild(path), defaultValue);
	}

	/**
	 * @return child {@code Config} if it exists, {@link Config#EMPTY} config otherwise
	 */
	default Config getChild(String path) {
		checkPath(path);
		Config config = this;
		for (String key : path.split(Pattern.quote(DELIMITER))) {
			if (key.isEmpty()) {
				continue;
			}
			Map<String, Config> children = config.getChildren();
			config = children.containsKey(key) ? children.get(key) : config.provideNoKeyChild(key);
		}
		return config;
	}

	default Config provideNoKeyChild(String key) {
		checkArgument(!getChildren().containsKey(key), "Children already contain key '%s'", key);
		return EMPTY;
	}

	/**
	 * Applies setter to value in path using converter to get it
	 *
	 * @param <T> type of value
	 */
	default <T> void apply(ConfigConverter<T> converter, String path, Consumer<T> setter) {
		checkPath(path);
		T value = get(converter, path);
		setter.accept(value);
	}

	/**
	 * The same as {@link Config#apply(ConfigConverter, String, Consumer)} but with default value
	 */
	default <T> void apply(ConfigConverter<T> converter, String path, T defaultValue, Consumer<T> setter) {
		apply(converter, path, defaultValue, (value, $) -> setter.accept(value));
	}

	/**
	 * The same as {@link Config#apply(ConfigConverter, String, Consumer)} but with default value and BiConsumer
	 * <p>Note that BiConsumer receives value and defaultValue as arguments</p>
	 */
	default <T> void apply(ConfigConverter<T> converter, String path, T defaultValue, BiConsumer<T, T> setter) {
		checkPath(path);
		T value = get(converter, path, defaultValue);
		setter.accept(value, defaultValue);
	}

	static <T> BiConsumer<T, T> ifNotDefault(Consumer<T> setter) {
		return (value, defaultValue) -> {
			if (!Objects.equals(value, defaultValue)) {
				setter.accept(value);
			}
		};
	}

	static <T> Consumer<T> ifNotNull(Consumer<T> setter) {
		return value -> {
			if (value != null) {
				setter.accept(value);
			}
		};
	}

	static <T> Consumer<T> ifNotDefault(T defaultValue, Consumer<T> setter) {
		return value -> {
			if (!Objects.equals(value, defaultValue)) {
				setter.accept(value);
			}
		};
	}

	/**
	 * @return empty config
	 */
	static Config create() {
		return EMPTY;
	}

	/**
	 * Creates a new config from all system properties
	 *
	 * @return new {@code Config}
	 */
	static Config ofSystemProperties() {
		return ofProperties(System.getProperties());
	}

	/**
	 * Creates a new config from system properties that start with a prefix
	 *
	 * @return new {@code Config}
	 */
	static Config ofSystemProperties(String prefix) {
		//noinspection unchecked - properties are expected to have String keys and values
		return ofMap(System.getProperties().entrySet().stream()
				.map(e -> (Map.Entry<String, String>) (Map.Entry<?, ?>) e)
				.filter(entry -> entry.getKey().startsWith(prefix))
				.collect(Collectors.toMap(
						e -> e.getKey().length() == prefix.length() ? "" : e.getKey().substring(prefix.length() + 1),
						Map.Entry::getValue)));
	}

	/**
	 * Creates a new config from properties
	 *
	 * @return new {@code Config}
	 */
	static Config ofProperties(Properties properties) {
		//noinspection unchecked - properties are expected to have String keys and values
		return ofMap((Map<String, String>) (Map<?, ?>) properties);
	}

	/**
	 * The same as {@link Config#ofProperties(String, boolean)} but with optional=false
	 *
	 * @return new {@code Config}
	 */
	static Config ofProperties(String fileName) {
		return ofProperties(fileName, false);
	}

	/**
	 * @see Config#ofProperties(Path, boolean)
	 */
	static Config ofProperties(String fileName, boolean optional) {
		return ofProperties(Paths.get(fileName), optional);
	}

	static Config ofClassPathProperties(String fileName) {
		return ofClassPathProperties(fileName, Thread.currentThread().getContextClassLoader(), false);
	}

	static Config ofClassPathProperties(String fileName, ClassLoader classLoader) {
		return ofClassPathProperties(fileName, classLoader, false);
	}

	static Config ofClassPathProperties(String fileName, boolean optional) {
		return ofClassPathProperties(fileName, Thread.currentThread().getContextClassLoader(), optional);
	}

	static Config ofClassPathProperties(String fileName, ClassLoader classLoader, boolean optional) {
		Properties props = new Properties();
		if (fileName.startsWith("/")) {
			fileName = fileName.substring(1);
		}
		try (InputStream resource = classLoader.getResourceAsStream(fileName)) {
			if (resource == null) {
				throw new FileNotFoundException(fileName);
			}
			props.load(resource);
		} catch (IOException e) {
			if (optional) {
				logger.warn("Can't load properties file: {}", fileName);
			} else {
				throw new IllegalArgumentException("Failed to load required properties: " + fileName, e);
			}
		}
		return ofProperties(props);
	}

	/**
	 * Creates new config from file
	 *
	 * @param file     with properties
	 * @param optional if true will log warning "Can't load..." else throws exception
	 * @return new {@code Config}
	 */
	static Config ofProperties(Path file, boolean optional) {
		Properties props = new Properties();
		try (InputStream is = Files.newInputStream(file)) {
			props.load(is);
		} catch (IOException e) {
			if (optional) {
				logger.warn("Can't load properties file: {}", file);
			} else {
				throw new IllegalArgumentException("Failed to load required properties: " + file, e);
			}
		}
		return ofProperties(props);
	}

	/**
	 * Creates config from Map
	 *
	 * @param map of path, value pairs
	 * @return new {@code Config}
	 */
	static Config ofMap(Map<String, String> map) {
		Config config = create();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			config = config.with(entry.getKey(), entry.getValue());
		}
		return config;
	}

	static Config ofConfigs(Map<String, Config> map) {
		Config config = create();
		for (Map.Entry<String, Config> entry : map.entrySet()) {
			config = config.with(entry.getKey(), entry.getValue());
		}
		return config;
	}

	/**
	 * @return new {@code Config} with only one value
	 */
	static Config ofValue(String value) {
		return create().with(THIS, value);
	}

	/**
	 * @param configConverter specifies converter for &lt;T&gt;
	 * @param value           of type &lt;T&gt;
	 * @return new {@code Config} with given value
	 */
	static <T> Config ofValue(ConfigConverter<T> configConverter, T value) {
		EffectiveConfig effectiveConfig = EffectiveConfig.wrap(Config.create());
		configConverter.get(effectiveConfig, value);
		return ofMap(effectiveConfig.getEffectiveDefaults());
	}

	static Config lazyConfig(Supplier<Config> configSupplier) {
		return new Config() {
			private volatile Config actualConfig;

			private Config ensureConfig() {
				if (actualConfig == null) {
					synchronized (this) {
						if (actualConfig == null) {
							actualConfig = configSupplier.get();
						}
					}
				}
				return actualConfig;
			}

			@Override
			public String getValue(@Nullable String defaultValue) {
				return ensureConfig().getValue(defaultValue);
			}

			@Override
			public String getValue() throws NoSuchElementException {
				return ensureConfig().getValue();
			}

			@Override
			public Map<String, Config> getChildren() {
				return ensureConfig().getChildren();
			}
		};
	}

	/**
	 * @param path path
	 * @return new {@code Config} with value in path
	 */
	default Config with(@NotNull String path, @NotNull String value) {
		checkPath(path);
		return with(path, new Config() {
			@Override
			public String getValue(@Nullable String defaultValue) {
				return value;
			}

			@Override
			public String getValue() throws NoSuchElementException {
				return value;
			}

			@Override
			public Map<String, Config> getChildren() {
				return emptyMap();
			}
		});
	}

	/**
	 * @param path   path
	 * @param config holds one value at root
	 * @return new {@code Config} with overridden value in path
	 * this method returns new config instead of changing the old one.
	 */
	default Config with(@NotNull String path, @NotNull Config config) {
		checkPath(path);
		String[] keys = path.split(Pattern.quote(DELIMITER));
		for (int i = keys.length - 1; i >= 0; i--) {
			String key = keys[i];
			if (key.isEmpty()) {
				continue;
			}
			Map<String, Config> map = singletonMap(key, config);
			config = new Config() {
				@Override
				public @Nullable String getValue(@Nullable String defaultValue) {
					return defaultValue;
				}

				@Override
				public String getValue() throws NoSuchElementException {
					throw new NoSuchElementException("No value at intermediate config node");
				}

				@Override
				public Map<String, Config> getChildren() {
					return map;
				}
			};
		}
		return overrideWith(config);
	}

	/**
	 * @param other config with values
	 * @return new {@code Config} with values from this config overridden by values from other
	 * this method returns new config instead of changing the old one.
	 */
	default Config overrideWith(Config other) {
		String otherValue = other.getValue(null);
		Map<String, Config> otherChildren = other.getChildren();
		if (otherValue == null && otherChildren.isEmpty()) {
			return this;
		}
		String value = otherValue != null ? otherValue : getValue(null);
		Map<String, Config> children = new LinkedHashMap<>(getChildren());
		otherChildren.forEach((key, otherChild) -> children.merge(key, otherChild, Config::overrideWith));
		Map<String, Config> finalChildren = unmodifiableMap(children);
		return new Config() {
			@Override
			public @Nullable String getValue(@Nullable String defaultValue) {
				return nonNullElse(value, defaultValue);
			}

			@Override
			public String getValue() throws NoSuchElementException {
				return nonNullOrException(value, () -> new NoSuchElementException("No value at config node"));
			}

			@Override
			public Map<String, Config> getChildren() {
				return finalChildren;
			}
		};
	}

	/**
	 * Tries to merge two configs into one. Throws {@code IllegalArgumentException} if there are conflicts.
	 *
	 * @param other config to merge with
	 * @return new merged {@code Config}
	 * this method returns new config instead of changing the old one.
	 */
	default Config combineWith(Config other) {
		String thisValue = getValue(null);
		String otherValue = other.getValue(null);
		if (thisValue != null && otherValue != null) {
			throw new IllegalArgumentException("Duplicate values\n" + this.toMap() + "\n" + other.toMap());
		}
		Map<String, Config> children = new LinkedHashMap<>(getChildren());
		other.getChildren().forEach((key, otherChild) -> children.merge(key, otherChild, Config::combineWith));
		return Config.EMPTY
				.overrideWith(thisValue != null ? Config.ofValue(thisValue) : Config.EMPTY)
				.overrideWith(otherValue != null ? Config.ofValue(otherValue) : Config.EMPTY)
				.overrideWith(Config.ofConfigs(children));
	}

	/**
	 * Converts this config to {@code Map<String, String>}
	 *
	 * @return new {@code Map<path, value>} where path and value are Strings
	 */
	default Map<String, String> toMap() {
		Map<String, String> result = new LinkedHashMap<>();
		if (hasValue()) {
			result.put(THIS, getValue());
		}
		Map<String, Config> children = getChildren();
		for (Map.Entry<String, Config> entry : children.entrySet()) {
			Map<String, String> childMap = entry.getValue().toMap();
			result.putAll(childMap.entrySet().stream()
					.collect(Collectors.toMap(e -> concatPath(entry.getKey(), e.getKey()), Map.Entry::getValue)));
		}
		return result;
	}

	/**
	 * Converts this config to {@code Properties}
	 *
	 * @return Properties with config values
	 */
	default Properties toProperties() {
		Properties properties = new Properties();
		toMap().forEach(properties::setProperty);
		return properties;
	}

}
