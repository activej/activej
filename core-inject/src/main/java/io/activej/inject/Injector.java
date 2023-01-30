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

package io.activej.inject;

import io.activej.inject.binding.*;
import io.activej.inject.impl.CompiledBinding;
import io.activej.inject.impl.CompiledBindingLocator;
import io.activej.inject.impl.Preprocessor;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.inject.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.binding.BindingGenerators.combinedGenerator;
import static io.activej.inject.binding.BindingGenerators.refusing;
import static io.activej.inject.binding.BindingTransformers.combinedTransformer;
import static io.activej.inject.binding.BindingTransformers.identity;
import static io.activej.inject.binding.BindingType.EAGER;
import static io.activej.inject.binding.BindingType.TRANSIENT;
import static io.activej.inject.binding.Multibinders.combinedMultibinder;
import static io.activej.inject.binding.Multibinders.errorOnDuplicate;
import static io.activej.inject.util.Utils.getScopeDisplayString;
import static io.activej.inject.util.Utils.next;
import static io.activej.types.Types.parameterizedType;
import static java.util.stream.Collectors.toMap;

/**
 * Injector is the main working component of the ActiveJ Inject.
 * <p>
 * It stores a trie of binding graphs and a cache of already made singletons.
 * <p>
 * Each injector is associated with exactly zero or one instance per {@link Key}.
 * <p>
 * Injector uses binding graph at the root of the trie to recursively create and then store instances of objects
 * associated with some {@link Key keys}.
 * Branches of the trie are used to {@link #enterScope enter scopes}.
 */
@SuppressWarnings({"unused", "WeakerAccess", "rawtypes"})
public final class Injector implements ResourceLocator {
	public record ScopeLocalData(Scope[] scope,
								  Map<Key<?>, Binding<?>> bindings,
								  Map<Key<?>, CompiledBinding<?>> compiledBindings,
								  Map<Key<?>, Integer> slotMapping, int slots,
								  CompiledBinding<?>[] eagerSingletons) {
	}

	final @Nullable Injector parent;
	final Trie<Scope, ScopeLocalData> scopeDataTree;

	final Map<Key<?>, Integer> localSlotMapping;
	final Map<Key<?>, CompiledBinding<?>> localCompiledBindings;

	final AtomicReferenceArray[] scopeCaches;

	private static Supplier<UnaryOperator<CompiledBinding<?>>> bytecodePostprocessorFactory = UnaryOperator::identity;

	/**
	 * Enables specialization of compiled bindings. <b>Depends on {@code ActiveJ-Specializer} module</b>
	 */
	@SuppressWarnings("unchecked")
	public static void useSpecializer() {
		try {
			Class<?> aClass = Class.forName("io.activej.specializer.Utils$InjectorSpecializer");
			Constructor<?> constructor = aClass.getConstructor();
			constructor.setAccessible(true);
			Object specializerInstance = constructor.newInstance();
			UnaryOperator<CompiledBinding<?>> specializer = (UnaryOperator<CompiledBinding<?>>) specializerInstance;
			Injector.bytecodePostprocessorFactory = () -> specializer;
		} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException | ClassNotFoundException | InstantiationException e) {
			throw new UnsupportedOperationException("Cannot access ActiveJ Specializer", e);
		}
	}

	@SuppressWarnings("unchecked")
	private Injector(@Nullable Injector parent, Trie<Scope, ScopeLocalData> scopeDataTree) {
		this.parent = parent;
		this.scopeDataTree = scopeDataTree;

		ScopeLocalData data = scopeDataTree.get();

		this.localSlotMapping = data.slotMapping;
		this.localCompiledBindings = data.compiledBindings;

		AtomicReferenceArray[] scopeCaches = parent == null ?
				new AtomicReferenceArray[1] :
				Arrays.copyOf(parent.scopeCaches, parent.scopeCaches.length + 1);

		AtomicReferenceArray localCache = new AtomicReferenceArray(data.slots);
		localCache.set(0, this);
		scopeCaches[scopeCaches.length - 1] = localCache;

		this.scopeCaches = scopeCaches;
	}

	/**
	 * This constructor combines given modules and then {@link #compile(Injector, Module) compiles} them.
	 */
	public static Injector of(Module... modules) {
		return compile(null, Modules.combine(modules));
	}

	public static Injector of(@Nullable Injector parent, Module... modules) {
		return compile(parent, Modules.combine(modules));
	}

	/**
	 * This constructor is a shortcut for threadsafe {@link #compile(Injector, Scope[], Trie, Multibinder, BindingTransformer, BindingGenerator) compile}
	 * with no instance overrides and no multibinders, transformers or generators.
	 */
	public static Injector of(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return compile(null, UNSCOPED,
				bindings.map(map -> map.entrySet().stream().collect(toMap(Entry::getKey, entry -> Set.of(entry.getValue())))),
				errorOnDuplicate(),
				identity(),
				refusing());
	}

	/**
	 * This constructor threadsafely {@link #compile(Injector, Scope[], Trie, Multibinder, BindingTransformer, BindingGenerator) compiles}
	 * given module, extracting bindings and their multibinders, transformers and generators from it, with no instance overrides
	 */
	public static Injector compile(@Nullable Injector parent, Module module) {
		return compile(parent, UNSCOPED, module.getBindings(),
				combinedMultibinder(module.getMultibinders()),
				combinedTransformer(module.getBindingTransformers()),
				combinedGenerator(module.getBindingGenerators()));
	}

	/**
	 * The most full-fledged compile method that allows you to create an Injector of any configuration.
	 * <p>
	 * Note that any injector <b>always</b> sets a binding of Injector key to provide itself.
	 *
	 * @param parent           parent injector that is called when this injector cannot fulfill the request
	 * @param scope            the scope of the injector, can be described as 'prefix of the root' of the binding trie,
	 *                         used when {@link #enterScope entering scopes}
	 * @param bindingsMultimap a trie of binding set graph with multiple possible conflicting bindings per key
	 *                         that are resolved as part of the compilation.
	 * @param multibinder      a multibinder that is called on every binding conflict (see {@link Multibinders#combinedMultibinder})
	 * @param transformer      a transformer that is called on every binding once (see {@link BindingTransformers#combinedTransformer})
	 * @param generator        a generator that is called on every missing binding (see {@link BindingGenerators#combinedGenerator})
	 * @see #enterScope
	 */
	public static Injector compile(@Nullable Injector parent,
			Scope[] scope,
			Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindingsMultimap,
			Multibinder<?> multibinder,
			BindingTransformer<?> transformer,
			BindingGenerator<?> generator) {

		Trie<Scope, Map<Key<?>, Binding<?>>> bindings = Preprocessor.reduce(bindingsMultimap, multibinder, transformer, generator);

		Set<Key<?>> known = new HashSet<>();
		known.add(Key.of(Injector.class)); // injector is hardcoded in and will always be present
		if (parent != null) {
			known.addAll(parent.localCompiledBindings.keySet());
		}

		Preprocessor.check(known, bindings);

		Trie<Scope, ScopeLocalData> scopeDataTree = compileBindingsTrie(
				parent != null ? parent.scopeCaches.length : 0,
				scope,
				bindings,
				parent != null ? parent.localCompiledBindings : Map.of()
		);
		return new Injector(parent, scopeDataTree);
	}

	private static Trie<Scope, ScopeLocalData> compileBindingsTrie(int scope, Scope[] path,
			Trie<Scope, Map<Key<?>, Binding<?>>> bindings,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsOfParent) {

		ScopeLocalData scopeLocalData = compileBindings(scope, path, bindings.get(), compiledBindingsOfParent);

		Map<Scope, Trie<Scope, ScopeLocalData>> children = new HashMap<>();

		bindings.getChildren().forEach((childScope, trie) -> {
			Map<Key<?>, CompiledBinding<?>> compiledBindingsCopy = new HashMap<>(compiledBindingsOfParent);
			compiledBindingsCopy.putAll(scopeLocalData.compiledBindings);
			children.put(childScope, compileBindingsTrie(scope + 1, next(path, childScope), bindings.get(childScope), compiledBindingsCopy));
		});

		return new Trie<>(scopeLocalData, children);
	}

	@SuppressWarnings("Convert2Lambda")
	private static ScopeLocalData compileBindings(int scope, Scope[] path,
			Map<Key<?>, Binding<?>> bindings,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsOfParent
	) {
		UnaryOperator<CompiledBinding<?>> postprocessor = bytecodePostprocessorFactory.get();

		boolean threadsafe = path.length == 0 || path[path.length - 1].isThreadsafe();

		Map<Key<?>, CompiledBinding<?>> compiledBindings = new HashMap<>();
		compiledBindings.put(Key.of(Injector.class), postprocessor.apply(scope == 0 ?
				new CompiledBinding<>() {
					volatile Object instance;

					@Override
					public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
						Object instance = this.instance;
						if (instance != null) return instance;
						this.instance = scopedInstances[scope].get(0);
						return this.instance;
					}
				} :
				new CompiledBinding<>() {
					@Override
					public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
						return scopedInstances[scope].get(0);
					}
				}));

		Map<Key<?>, Integer> slotMapping = new HashMap<>();
		slotMapping.put(Key.of(Injector.class), 0);

		int[] nextSlot = {1};

		List<CompiledBinding<?>> eagerSingletons = new ArrayList<>();

		for (Entry<Key<?>, Binding<?>> entry : bindings.entrySet()) {
			Key<?> key = entry.getKey();
			Binding<?> binding = entry.getValue();
			CompiledBinding<?> compiledBinding = compileBinding(
					postprocessor,
					scope, path, threadsafe,
					key, bindings,
					compiledBindings, compiledBindingsOfParent,
					slotMapping, nextSlot
			);
			if (binding.getType() == EAGER) {
				eagerSingletons.add(compiledBinding);
			}
		}

		bindings.put(Key.of(Injector.class), Binding.to(
						() -> {
							throw new AssertionError("Injector constructor must never be called since it's instance is always put in the cache manually");
						})
				.as(EAGER));

		compiledBindingsOfParent.forEach(compiledBindings::putIfAbsent);

		int size = nextSlot[0];
		nextSlot[0] = -1;

		return new ScopeLocalData(path, bindings, compiledBindings, slotMapping, size, eagerSingletons.toArray(new CompiledBinding[0]));
	}

	private static CompiledBinding<?> compileBinding(
			UnaryOperator<CompiledBinding<?>> postprocessor,
			int scope, Scope[] path, boolean threadsafe,
			Key<?> key, Map<Key<?>, Binding<?>> bindings,
			Map<Key<?>, CompiledBinding<?>> compiledBindings, Map<Key<?>, CompiledBinding<?>> compiledBindingsOfParent,
			Map<Key<?>, Integer> slotMapping, int[] nextSlot
	) {

		// not computeIfAbsent because of recursion
		CompiledBinding<?> already = compiledBindings.get(key);
		if (already != null) {
			return already;
		}
		if (nextSlot[0] == -1) {
			throw new DIException("Failed to locate a binding for " + key.getDisplayString() + " after scope " + getScopeDisplayString(path) + " was fully compiled");
		}

		Binding<?> binding = bindings.get(key);
		if (binding == null) {
			CompiledBinding<?> compiled = compiledBindingsOfParent.get(key);
			if (compiled == null) throw new AssertionError();
			compiledBindings.put(key, compiled);
			return compiled;
		}

		Integer index;
		if (binding instanceof BindingToKey || binding.getType() == TRANSIENT) {
			index = null;
		} else {
			slotMapping.put(key, index = nextSlot[0]++);
		}

		CompiledBinding<?> compiled = postprocessor.apply(binding.compile(
				new CompiledBindingLocator() {
					@SuppressWarnings("unchecked")
					@Override
					public <Q> CompiledBinding<Q> get(Key<Q> key) {
						return (CompiledBinding<Q>) compileBinding(
								postprocessor,
								scope,
								path,
								threadsafe,
								key,
								bindings,
								compiledBindings,
								compiledBindingsOfParent,
								slotMapping,
								nextSlot
						);
					}
				}, threadsafe, scope, index));

		if (binding instanceof BindingToKey) {
			Key<?> targetKey = ((BindingToKey<?>) binding).getKey();
			Integer targetIndex = slotMapping.get(targetKey);
			// should already be there, but may be null if a binding is transient
			if (targetIndex != null) {
				slotMapping.put(key, targetIndex);
			}
		}

		compiledBindings.put(key, compiled);

		return compiled;
	}

	/**
	 * Returns an instance for given key.
	 * At first call an instance is created once for non-transient bindings
	 * and next calls to this method will return the same instance.
	 * <p>
	 * This method throws an exception if a binding was not bound for given key,
	 * or if a binding refused to make an instance for some reason (returned <code>null</code>).
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> T getInstance(Key<T> key) {
		CompiledBinding<?> binding = localCompiledBindings.get(key);
		if (binding == null) {
			throw DIException.cannotConstruct(key, null);
		}
		Object instance = binding.getInstance(scopeCaches, -1);
		//noinspection ConstantConditions - still have to check
		if (instance == null) {
			throw DIException.cannotConstruct(key, scopeDataTree.get().bindings.get(key));
		}
		return (T) instance;
	}

	/**
	 * @see #getInstance(Key)
	 */
	@Override
	public <T> T getInstance(Class<T> type) {
		return getInstance(Key.ofType(type));
	}

	/**
	 * Same as {@link #getInstance(Key)} except that it returns <code>null</code> instead of throwing an exception.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> @Nullable T getInstanceOrNull(Key<T> key) {
		CompiledBinding<?> binding = localCompiledBindings.get(key);
		return binding != null ? (T) binding.getInstance(scopeCaches, -1) : null;
	}

	/**
	 * @see #getInstanceOrNull(Key)
	 */
	@Override
	public <T> @Nullable T getInstanceOrNull(Class<T> type) {
		return getInstanceOrNull(Key.of(type));
	}

	/**
	 * Same as {@link #getInstanceOrNull(Key)}, but replaces <code>null</code> with given default value.
	 */
	@Override
	public <T> T getInstanceOr(Key<T> key, T defaultValue) {
		T instance = getInstanceOrNull(key);
		return instance != null ? instance : defaultValue;
	}

	/**
	 * @see #getInstanceOr(Key, Object)
	 */
	@Override
	public <T> T getInstanceOr(Class<T> type, T defaultValue) {
		return getInstanceOr(Key.of(type), defaultValue);
	}

	/**
	 * A shortcut for <code>getInstance(new Key&lt;InstanceProvider&lt;T&gt;&gt;(){})</code>
	 */
	public <T> InstanceProvider<T> getInstanceProvider(Key<T> key) {
		return getInstance(Key.ofType(parameterizedType(InstanceProvider.class, key.getType()), key.getQualifier()));
	}

	/**
	 * A shortcut for <code>getInstanceProvider(Key.of(type))</code>
	 *
	 * @see #getInstanceProvider(Key)
	 */
	public <T> InstanceProvider<T> getInstanceProvider(Class<T> type) {
		return getInstanceProvider(Key.of(type));
	}

	/**
	 * A shortcut for <code>getInstance(new Key&lt;InstanceInjector&lt;T&gt;&gt;(){})</code>
	 */
	public <T> InstanceInjector<T> getInstanceInjector(Key<T> key) {
		return getInstance(Key.ofType(parameterizedType(InstanceInjector.class, key.getType()), key.getQualifier()));
	}

	/**
	 * A shortcut for <code>getInstanceInjector(Key.of(type))</code>
	 *
	 * @see #getInstanceInjector(Key)
	 */
	public <T> InstanceInjector<T> getInstanceInjector(Class<T> type) {
		return getInstanceInjector(Key.of(type));
	}

	/**
	 * A shortcut for <code>getInstance(new Key&lt;OptionalDependency&lt;T&gt;&gt;(){})</code>
	 */
	public <T> OptionalDependency<T> getOptionalDependency(Key<T> key) {
		return getInstance(Key.ofType(parameterizedType(OptionalDependency.class, key.getType()), key.getQualifier()));
	}

	/**
	 * A shortcut for <code>getOptionalDependency(Key.of(type))</code>
	 *
	 * @see #getOptionalDependency(Key)
	 */
	public <T> OptionalDependency<T> getOptionalDependency(Class<T> type) {
		return getOptionalDependency(Key.of(type));
	}

	public void createEagerInstances() {
		for (CompiledBinding<?> compiledBinding : scopeDataTree.get().eagerSingletons) {
			compiledBinding.getInstance(scopeCaches, -1);
		}
	}

	/**
	 * This method returns an instance only if it already was created by a {@link #getInstance} call before,
	 * it does not trigger instance creation.
	 */
	@SuppressWarnings("unchecked")
	public <T> @Nullable T peekInstance(Key<T> key) {
		Integer index = localSlotMapping.get(key);
		return index != null ? (T) scopeCaches[scopeCaches.length - 1].get(index) : null;
	}

	/**
	 * This method returns an instance only if it already was created by a {@link #getInstance} call before,
	 * it does not trigger instance creation.
	 *
	 * @see #peekInstance(Key)
	 */
	public <T> @Nullable T peekInstance(Class<T> type) {
		return peekInstance(Key.of(type));
	}

	/**
	 * This method checks if an instance for this key was created by a {@link #getInstance} call before.
	 */
	public boolean hasInstance(Key<?> key) {
		return peekInstance(key) != null;
	}

	/**
	 * This method checks if an instance for this key was created by a {@link #getInstance} call before.
	 *
	 * @see #hasInstance(Key)
	 */
	public boolean hasInstance(Class<?> type) {
		return peekInstance(type) != null;
	}

	/**
	 * This method returns a copy of the injector cache - a map of all already created non-transient instances at the current scope.
	 */
	public Map<Key<?>, Object> peekInstances() {
		Map<Key<?>, Object> result = new HashMap<>();
		AtomicReferenceArray scopeCache = scopeCaches[scopeCaches.length - 1];
		for (Entry<Key<?>, Integer> entry : localSlotMapping.entrySet()) {
			Object value = scopeCache.get(entry.getValue());
			if (value != null) {
				result.put(entry.getKey(), value);
			}
		}
		return result;
	}

	/**
	 * This method puts an instance into a cache slot of given key,
	 * meaning that already existing instance would be replaced or a binding would never be actually called.
	 * <p>
	 * Use this at your own risk, this allows high control over the injector, but can be easily abused.
	 */
	@SuppressWarnings("unchecked")
	public <T> void putInstance(Key<T> key, T instance) {
		Integer index = localSlotMapping.get(key);
		if (index == null) {
			throw DIException.noCachedBinding(key, getScope());
		}
		scopeCaches[scopeCaches.length - 1].lazySet(index, instance);
	}

	/**
	 * This method puts an instance into a cache slot of given key,
	 * meaning that already existing instance would be replaced or a binding would never be actually called.
	 * <p>
	 * Use this at your own risk, this allows high control over the injector, but can be easily abused.
	 *
	 * @see #putInstance(Key, Object)
	 */
	public <T> void putInstance(Class<T> key, T instance) {
		putInstance(Key.of(key), instance);
	}

	public @Nullable Binding<?> getBinding(Class<?> type) {
		return getBinding(Key.of(type));
	}

	public @Nullable Binding<?> getBinding(Key<?> key) {
		return scopeDataTree.get().bindings.get(key);
	}

	/**
	 * This method returns true if a binding was bound for given key.
	 */
	public boolean hasBinding(Key<?> key) {
		return scopeDataTree.get().bindings.containsKey(key);
	}

	/**
	 * This method returns true if a binding was bound for given class key.
	 *
	 * @see #hasBinding(Key)
	 */
	public boolean hasBinding(Class<?> type) {
		return hasBinding(Key.of(type));
	}

	/**
	 * Creates an injector that operates on a binding graph at a given prefix (scope) of the binding graph trie and this injector as its parent.
	 */
	public Injector enterScope(Scope scope) {
		return new Injector(this, scopeDataTree.get(scope));
	}

	public @Nullable Injector getParent() {
		return parent;
	}

	public Scope[] getScope() {
		return scopeDataTree.get().scope;
	}

	/**
	 * This method returns bindings for current scope
	 * <p>
	 * Note that this method expensive to call repeatedly
	 */
	public Map<Key<?>, Binding<?>> getBindings() {
		return scopeDataTree.get().bindings;
	}

	/**
	 * This method returns a trie of bindings
	 * <p>
	 * Note that this method expensive to call repeatedly
	 */
	public Trie<Scope, Map<Key<?>, Binding<?>>> getBindingsTrie() {
		return scopeDataTree.map(graph -> graph.bindings);
	}

	@Override
	public String toString() {
		return "Injector{scope=" + getScopeDisplayString(scopeDataTree.get().scope) + '}';
	}
}
