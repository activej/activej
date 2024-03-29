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

package io.activej.common;

import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static io.activej.common.Checks.checkState;

/**
 * A class for initializing runtime constants
 * <p>
 * Each application setting provides a name of a class, a name of a setting, and a default value to be
 * used in case a setting is not explicitly set
 * <p>
 * A setting is searched by using both fully qualified class name and a class's simple name
 * <p>
 * By default, system properties are searched to find whether a setting is explicitly set.
 * However, alternative properties may be used as a source.
 * Individual settings may also be set programmatically.
 * <p>
 * After any setting has been looked up it is not allowed to change properties source or
 * update individual settings. Hence, all configuration should preferably happen
 * in a static initialization block before constants have been initialized.
 */
public final class ApplicationSettings {
	private static final Map<Class<?>, Map<String, Object>> customSettings = new HashMap<>();

	private static Properties properties = System.getProperties();
	private static volatile boolean firstLookupDone = false;

	/**
	 * Uses alternative properties to look up settings
	 * <p>
	 * This method should not be called after any setting look up has been made
	 *
	 * @param properties alternative properties
	 */
	public static void useProperties(Properties properties) {
		ensureNotLookedUp();
		ApplicationSettings.properties = properties;
	}

	/**
	 * Sets a custom setting
	 * <p>
	 * This setting will override settings from properties
	 * <p>
	 * This method should not be called after any setting look up has been made
	 *
	 * @param type  a class referenced by a setting
	 * @param name  a name of a setting
	 * @param value a value of a setting
	 */
	public static void set(Class<?> type, String name, Object value) {
		ensureNotLookedUp();
		customSettings.computeIfAbsent(type, $ -> new HashMap<>()).put(name, value);
	}

	/**
	 * Retrieves a setting. This is a base method that can be used to parse a setting string
	 * into an arbitrary value.
	 * <p>
	 * A setting is searched by using both fully qualified class name and a class's simple name
	 *
	 * @param parser   function that transforms a setting string into a value
	 * @param type     a class referenced by a setting
	 * @param name     a name of a setting
	 * @param defValue default value that will be used if a setting property is not found
	 */
	@Contract("_, _, _, !null -> !null")
	public static <T> @Nullable T get(Function<String, T> parser, Class<?> type, String name, @Nullable T defValue) {
		checkState(!type.isAnonymousClass(), "Anonymous classes cannot be used for application settings");

		firstLookupDone = true;
		//noinspection unchecked
		T customSetting = (T) customSettings.getOrDefault(type, Map.of()).get(name);
		if (customSetting != null) {
			return customSetting;
		}
		String property = getProperty(type, name);
		if (property != null) {
			return parser.apply(property);
		}
		return defValue;
	}

	/**
	 * Retrieves a {@link String} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable String getString(Class<?> type, String name, @Nullable String defValue) {
		return get(Function.identity(), type, name, defValue);
	}

	/**
	 * Retrieves an {@code int} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable Integer getInt(Class<?> type, String name, Integer defValue) {
		return get(Integer::parseInt, type, name, defValue);
	}

	/**
	 * Retrieves a {@code long} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable Long getLong(Class<?> type, String name, Long defValue) {
		return get(Long::parseLong, type, name, defValue);
	}

	/**
	 * Retrieves a {@code boolean} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable Boolean getBoolean(Class<?> type, String name, Boolean defValue) {
		return get(ApplicationSettings::parseBoolean, type, name, defValue);
	}

	public static boolean parseBoolean(String s) {
		return s.trim().isEmpty() || Boolean.parseBoolean(s);
	}

	/**
	 * Retrieves a {@code double} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable Double getDouble(Class<?> type, String name, Double defValue) {
		return get(Double::parseDouble, type, name, defValue);
	}

	/**
	 * Retrieves a {@link Duration} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 * @see Duration#parse(CharSequence)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable Duration getDuration(Class<?> type, String name, @Nullable Duration defValue) {
		return get(StringFormatUtils::parseDuration, type, name, defValue);
	}

	/**
	 * Retrieves a {@link MemSize} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable MemSize getMemSize(Class<?> type, String name, @Nullable MemSize defValue) {
		return get(MemSize::valueOf, type, name, defValue);
	}

	/**
	 * Retrieves a {@link InetSocketAddress} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable InetSocketAddress getInetSocketAddress(Class<?> type, String name, @Nullable InetSocketAddress defValue) {
		return get(address -> {
			try {
				return StringFormatUtils.parseInetSocketAddressResolving(address);
			} catch (MalformedDataException e) {
				throw new RuntimeException("Malformed inet socket address: " + address, e);
			}
		}, type, name, defValue);
	}

	/**
	 * Retrieves a {@link Charset} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 * @see Charset#forName(String)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable Charset getCharset(Class<?> type, String name, @Nullable Charset defValue) {
		return get(charset -> {
			try {
				return Charset.forName(charset);
			} catch (UnsupportedCharsetException e) {
				throw new RuntimeException("Unsupported charset: " + charset, e);
			}
		}, type, name, defValue);
	}

	/**
	 * Retrieves a {@link Locale} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 * @see Locale#forLanguageTag(String)
	 */
	@Contract("_, _, !null -> !null")
	public static @Nullable Locale getLocale(Class<?> type, String name, @Nullable Locale defValue) {
		return get(Locale::forLanguageTag, type, name, defValue);
	}

	/**
	 * Retrieves a {@link Enum} setting
	 *
	 * @see #get(Function, Class, String, Object)
	 * @see Enum#valueOf(Class, String)
	 */
	@Contract("_, _, _, !null -> !null")
	public static <E extends Enum<E>> @Nullable E getEnum(Class<?> type, String name, Class<E> enumClass, @Nullable E defValue) {
		return get(val -> Enum.valueOf(enumClass, val), type, name, defValue);
	}

	private static @Nullable String getProperty(Class<?> type, String name) {
		String property;
		property = properties.getProperty(type.getName() + "." + name);
		if (property != null) return property;
		property = properties.getProperty(type.getSimpleName() + "." + name);
		return property;
	}

	private static void ensureNotLookedUp() {
		if (firstLookupDone) {
			throw new IllegalStateException(
				"Attempting to update application settings after some of them have been retrieved\n" +
				"All updates should happen prior to any constant initialization via ApplicationSettings, " +
				"preferably in static initialization block of 'main' class");
		}
	}
}
