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

package io.activej.codegen;

import io.activej.common.builder.AbstractBuilder;
import org.jetbrains.annotations.Nullable;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.codegen.util.Utils.getPathSetting;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

/**
 * A {@link ClassLoader} for defining dynamically generated classes.
 * Also supports in memory caching as well as persistent caching of defined classes.
 * <p>
 * To simply define a new class from a bytecode use {@link #defineClass(String, byte[])} method.
 * <p>
 * For in memory caching of classes use {@link #ensureClass(ClassKey, Supplier)} and
 * {@link #ensureClass(ClassKey, Function)} methods.
 * <p>
 * As an alternative for in memory caching you may use {@link #ensureClass(String, Supplier)} and
 * {@link #ensureClass(String, BiFunction)} methods without specifying a {@link #bytecodeStorage}.
 * <p>
 * For persistent caching of classes you need to use {@link #ensureClass(String, Supplier)} and
 * {@link #ensureClass(String, BiFunction)} methods and also specify a persistent {@link IBytecodeStorage} using
 * {@link Builder#withBytecodeStorage(IBytecodeStorage)} method.
 */
@SuppressWarnings("WeakerAccess")
public final class DefiningClassLoader extends ClassLoader implements DefiningClassLoaderMBean {
	public static final Path DEFAULT_DEBUG_OUTPUT_DIR = getPathSetting(DefiningClassLoader.class, "debugOutputDir", null);

	private final Map<String, Class<?>> definedClasses = new ConcurrentHashMap<>();
	private final Map<ClassKey<?>, AtomicReference<Class<?>>> cachedClasses = new ConcurrentHashMap<>();

	private @Nullable IBytecodeStorage bytecodeStorage;

	private Path debugOutputDir = DEFAULT_DEBUG_OUTPUT_DIR;

	private DefiningClassLoader() {
	}

	private DefiningClassLoader(ClassLoader parent) {
		super(parent);
	}

	/**
	 * Creates a new instance of {@code DefiningClassLoader}
	 * with system class loader as a parent class loader
	 *
	 * @return a new instance of a {@code DefiningClassLoader}
	 */
	public static DefiningClassLoader create() {
		return builder().build();
	}

	/**
	 * Creates a new instance of {@code DefiningClassLoader}
	 * with given class loader as a parent class loader
	 *
	 * @param parent parent class loader
	 * @return a new instance of a {@code DefiningClassLoader}
	 */
	public static DefiningClassLoader create(ClassLoader parent) {
		return builder(parent).build();
	}

	public static Builder builder() {
		return new DefiningClassLoader().new Builder();
	}

	public static Builder builder(ClassLoader parent) {
		return new DefiningClassLoader(parent).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, DefiningClassLoader> {
		private Builder() {}

		/**
		 * Adds a persistent cache for the bytecode that is used for defining classes.
		 *
		 * @param bytecodeStorage a persistent storage of bytecode
		 * @see IBytecodeStorage
		 */
		public Builder withBytecodeStorage(IBytecodeStorage bytecodeStorage) {
			checkNotBuilt(this);
			DefiningClassLoader.this.bytecodeStorage = bytecodeStorage;
			return this;
		}

		/**
		 * Writes all classes to the specified directory once a class is defined.
		 * <p>
		 * If a directory does not exist when class is defined, a runtime error will be thrown.
		 *
		 * @param debugOutputDir directory where bytecode would be written to for debug purposes
		 */
		public Builder withDebugOutputDir(Path debugOutputDir) {
			checkNotBuilt(this);
			DefiningClassLoader.this.debugOutputDir = debugOutputDir;
			return this;
		}

		@Override
		protected DefiningClassLoader doBuild() {
			return DefiningClassLoader.this;
		}
	}

	/**
	 * Defines a class using a given class name and bytecode
	 *
	 * @param className name of a defined class
	 * @param bytecode  bytecode of a defined class
	 * @return newly defined class
	 */
	public Class<?> defineClass(String className, byte[] bytecode) {
		Class<?> aClass = super.defineClass(className, bytecode, 0, bytecode.length);
		definedClasses.put(className, aClass);
		if (debugOutputDir != null) {
			try (FileOutputStream fos = new FileOutputStream(debugOutputDir.resolve(className + ".class").toFile())) {
				fos.write(bytecode);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return aClass;
	}

	/**
	 * Ensures that a class of a given name is present
	 *
	 * @see #ensureClass(String, BiFunction)
	 */
	public <T> Class<T> ensureClass(String className, Supplier<ClassGenerator<T>> classGenerator) {
		return ensureClass(className, (cl, s) -> classGenerator.get().generateBytecode(cl, s));
	}

	/**
	 * Ensures that a class of a given name is present
	 *
	 * @see #ensureClass(String, BiFunction)
	 */
	public <T> Class<T> ensureClass(ClassKey<T> key, Supplier<ClassGenerator<T>> classGenerator) {
		return ensureClass(key, classLoader -> classGenerator.get().generateBytecode(classLoader));
	}

	/**
	 * Ensures that a class of a given name is present
	 *
	 * @see #ensureClass(String, BiFunction)
	 */
	public <T> T ensureClassAndCreateInstance(
		String className, Supplier<ClassGenerator<T>> classGenerator, Object... arguments
	) {
		return createInstance(ensureClass(className, classGenerator), arguments);
	}

	/**
	 * Ensures that a class of a given name is present and creates a new instance of such class
	 *
	 * @see #ensureClass(String, BiFunction)
	 */
	public <T> T ensureClassAndCreateInstance(
		ClassKey<T> key, Supplier<ClassGenerator<T>> classGenerator, Object... arguments
	) {
		Class<T> aClass = ensureClass(key, classGenerator);
		return createInstance(aClass, arguments);
	}

	/**
	 * Ensures that a class of a given name is present either by loading it from a {@link IBytecodeStorage} or
	 * by creating the class using given bytecode factory.
	 * <p>
	 * If a persistent {@link IBytecodeStorage} is not set, a built-in in memory cache will be used. Which means that
	 * a bytecode factory will be called at most once.
	 * <p>
	 * If a persistent {@link IBytecodeStorage} is set, a generated bytecode would be stored in the storage. This way
	 * the cache would survive application restarts, which would allow optimizing startup time.
	 *
	 * @param className       a desired name of a class
	 * @param bytecodeBuilder factory that creates a {@link GeneratedBytecode} out of {@code this} {@link DefiningClassLoader}
	 *                        and a class name
	 * @param <T>             type parameter that represents ensured class
	 * @return an ensured class
	 */
	@SuppressWarnings("unchecked")
	public <T> Class<T> ensureClass(String className, BiFunction<ClassLoader, String, GeneratedBytecode> bytecodeBuilder) {
		try {
			return (Class<T>) loadClass(className, false);
		} catch (ClassNotFoundException ignored) {
		}

		synchronized (getClassLoadingLock(className)) {
			if (bytecodeStorage != null) {
				byte[] bytecode = bytecodeStorage.loadBytecode(className).orElse(null);
				if (bytecode != null) {
					return (Class<T>) defineClass(className, bytecode);
				}
			}

			try (GeneratedBytecode generatedBytecode = bytecodeBuilder.apply(this, className)) {
				Class<T> generatedClass = (Class<T>) generatedBytecode.generateClass(this);
				if (bytecodeStorage != null) {
					bytecodeStorage.saveBytecode(className, generatedBytecode.getBytecode());
				}
				return generatedClass;
			}
		}
	}

	/**
	 * Ensures that a class of a given name is present either by loading it from in memory cache or
	 * by creating the class using given bytecode factory.
	 * <p>
	 * Defined classes a stored in a cache by a {@link ClassKey}, which is a combination of some superclass as well as
	 * an array of some arbitrary arguments.
	 * <p>
	 * A bytecode factory will be called at most once. Classes ensured using this method
	 * are not persisted between application restarts.
	 *
	 * @param key             a key of a class
	 * @param bytecodeBuilder factory that creates a {@link GeneratedBytecode} out of {@code this} {@link DefiningClassLoader}
	 * @param <T>             type parameter that represents ensured class
	 * @return an ensured class
	 */
	public <T> Class<T> ensureClass(ClassKey<T> key, Function<ClassLoader, GeneratedBytecode> bytecodeBuilder) {
		AtomicReference<Class<?>> reference = cachedClasses.computeIfAbsent(key, k -> new AtomicReference<>());
		Class<?> generatedClass = reference.get();
		if (generatedClass == null) {
			synchronized (reference) {
				generatedClass = reference.get();
				if (generatedClass == null) {
					try (GeneratedBytecode generatedBytecode = bytecodeBuilder.apply(this)) {
						generatedClass = generatedBytecode.generateClass(this);
					}
					reference.set(generatedClass);
				}
			}
		}
		//noinspection unchecked
		return (Class<T>) generatedClass;
	}

	/**
	 * Ensures that a class of a given name is present and creates a new instance of such class
	 *
	 * @see #ensureClass(ClassKey, Function)
	 */
	public <T> T ensureClassAndCreateInstance(
		ClassKey<T> key, Function<ClassLoader, GeneratedBytecode> bytecodeBuilder, Object... arguments
	) {
		Class<T> aClass = ensureClass(key, bytecodeBuilder);
		return createInstance(aClass, arguments);
	}

	static <T> T createInstance(Class<T> aClass, Object[] arguments) {
		try {
			return aClass
				.getConstructor(Arrays.stream(arguments).map(Object::getClass).toArray(Class<?>[]::new))
				.newInstance(arguments);
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns a cached class by a class key
	 *
	 * @param key a class key
	 * @return a cached class
	 */
	public @Nullable Class<?> getCachedClass(ClassKey<?> key) {
		return Optional.ofNullable(cachedClasses.get(key)).map(AtomicReference::get).orElse(null);
	}

	// region JMX
	@Override
	public int getDefinedClassesCount() {
		return definedClasses.size();
	}

	@Override
	public Map<String, Long> getDefinedClassesCountByType() {
		return definedClasses.values().stream()
			.map(aClass -> aClass.getSuperclass() == Object.class && aClass.getInterfaces().length != 0 ?
				aClass.getInterfaces()[0] :
				aClass.getSuperclass())
			.map(Class::getName)
			.collect(groupingBy(identity(), counting()));
	}

	@Override
	public int getCachedClassesCount() {
		return cachedClasses.size();
	}

	@Override
	public Map<String, Long> getCachedClassesCountByType() {
		return cachedClasses.keySet().stream()
			.map(key -> key.getKeyClass().getName())
			.collect(groupingBy(identity(), counting()));
	}
	// endregion

	@Override
	public String toString() {
		return "{classes=" + cachedClasses.size() + ", byType=" + getCachedClassesCountByType() + '}';
	}
}
