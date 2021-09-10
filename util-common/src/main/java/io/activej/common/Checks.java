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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class is used for determining whether {@link Checks} should be enabled or not.
 * It is sometimes useful to disable preconditions checks at runtime environment
 * to squeeze out more performance in a tight loop.
 * <p>
 * The common pattern is having a
 * <pre>
 * {@code private static final boolean CHECK = Check.isEnabled(MyClass.class);}
 * </pre>
 * constant in your class. When using {@link Checks} just wrap the call in if-statement like so:
 * <pre>
 * {@code if (CHECK) Preconditions.checkNotNull(value);}
 * </pre>
 * <p>
 * By default, all checks are <b>disabled</b> (like java asserts).
 * To enable all checks you can run application with system property {@code -Dchk=on}
 * You can enable or disable checks for the whole package (with its subpackages) or for individual classes
 * like so: {@code -Dchk:io.activej.eventloop=on -Dchk:io.activej.promise.Promise=off}.
 */
public final class Checks {
	private static final boolean ENABLED_BY_DEFAULT;
	private static final String ENV_PREFIX = "chk:";

	static {
		String enabled = System.getProperty("chk");

		if (enabled == null || enabled.equals("off")) ENABLED_BY_DEFAULT = false;
		else if (enabled.equals("on")) ENABLED_BY_DEFAULT = true;
		else throw new RuntimeException(getErrorMessage(enabled));
	}

	/**
	 * Indicates whether checks are enabled or disabled for the specified class
	 *
	 * @param cls class to be checked
	 * @return {@code true} if checks are enabled for the given class, {@code false} otherwise
	 */
	public static boolean isEnabled(Class<?> cls) {
		checkState(!cls.isAnonymousClass(), "Anonymous classes cannot be used for checks");

		String property;
		String path = cls.getName();
		if ((property = System.getProperty(ENV_PREFIX + path)) == null) {
			property = System.getProperty(ENV_PREFIX + cls.getSimpleName());
			while (property == null) {
				int idx = path.lastIndexOf('.');
				if (idx == -1) break;
				path = path.substring(0, idx);
				property = System.getProperty(ENV_PREFIX + path);
			}
		}

		boolean enabled = ENABLED_BY_DEFAULT;
		if (property != null) {
			if (property.equals("on")) enabled = true;
			else if (property.equals("off")) enabled = false;
			else throw new RuntimeException(getErrorMessage(property));
		}
		return enabled;
	}

	public static boolean isEnabledByDefault() {
		return ENABLED_BY_DEFAULT;
	}

	public static @NotNull <T> T checkNotNull(@Nullable T reference) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException();
	}

	public static @NotNull <T> T checkNotNull(@Nullable T reference, Object message) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException(String.valueOf(message));
	}

	public static @NotNull <T> T checkNotNull(@Nullable T reference, Supplier<String> message) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException(message.get());
	}

	public static @NotNull <T> T checkNotNull(@Nullable T reference, String template, Object... args) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException(String.format(template, args));
	}

	public static void checkState(boolean expression) {
		if (!expression) {
			throw new IllegalStateException();
		}
	}

	public static void checkState(boolean expression, Object message) {
		if (!expression) {
			throw new IllegalStateException(String.valueOf(message));
		}
	}

	public static void checkState(boolean expression, Supplier<String> message) {
		if (!expression) {
			throw new IllegalStateException(message.get());
		}
	}

	public static void checkState(boolean expression, String template, Object... args) {
		if (!expression) {
			throw new IllegalStateException(String.format(template, args));
		}
	}

	public static void checkArgument(boolean expression) {
		if (!expression) {
			throw new IllegalArgumentException();
		}
	}

	public static void checkArgument(boolean expression, Object message) {
		if (!expression) {
			throw new IllegalArgumentException(String.valueOf(message));
		}
	}

	public static void checkArgument(boolean expression, Supplier<String> message) {
		if (!expression) {
			throw new IllegalArgumentException(message.get());
		}
	}

	public static void checkArgument(boolean expression, String template, Object... args) {
		if (!expression) {
			throw new IllegalArgumentException(String.format(template, args));
		}
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException();
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, Object message) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(String.valueOf(message));
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, Supplier<String> message) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(message.get());
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, Function<T, String> message) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(message.apply(argument));
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, String template, Object... args) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(String.format(template, args));
	}

	private static String getErrorMessage(String value) {
		return "Only 'on' and 'off' values are allowed for 'chk' system properties, was '" + value + '\'';
	}
}
