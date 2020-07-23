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
import io.activej.inject.impl.*;
import io.activej.inject.module.DefaultModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.inject.util.MarkedBinding;
import io.activej.inject.util.Trie;
import io.activej.inject.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.binding.BindingGenerator.combinedGenerator;
import static io.activej.inject.binding.BindingGenerator.refusing;
import static io.activej.inject.binding.BindingTransformer.combinedTransformer;
import static io.activej.inject.binding.BindingTransformer.identity;
import static io.activej.inject.binding.BindingType.*;
import static io.activej.inject.binding.Multibinder.combinedMultibinder;
import static io.activej.inject.binding.Multibinder.errorOnDuplicate;
import static io.activej.inject.impl.CompiledBinding.missingOptionalBinding;
import static io.activej.inject.util.Utils.getScopeDisplayString;
import static io.activej.inject.util.Utils.next;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;

/**
 * Injector is the main working component of the ActiveInject.
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
	private static final class ScopeLocalData {
		final Scope[] scope;
		final Map<Key<?>, BindingInfo> bindingInfo;
		final Map<Key<?>, CompiledBinding<?>> compiledBindings;
		final Map<Key<?>, Integer> slotMapping;
		final int slots;

		final CompiledBinding<?>[] eagerSingletons;

		private ScopeLocalData(
				Scope[] scope,
				Map<Key<?>, BindingInfo> bindingInfo,
				Map<Key<?>, CompiledBinding<?>> compiledBindings,
				Map<Key<?>, Integer> slotMapping,
				int slots,
				CompiledBinding<?>[] eagerSingletons
		) {
			this.scope = scope;
			this.bindingInfo = bindingInfo;
			this.compiledBindings = compiledBindings;
			this.slotMapping = slotMapping;
			this.slots = slots;
			this.eagerSingletons = eagerSingletons;
		}
	}

	@Nullable
	final Injector parent;
	final Trie<Scope, ScopeLocalData> scopeDataTree;

	final Map<Key<?>, Integer> localSlotMapping;
	final Map<Key<?>, CompiledBinding<?>> localCompiledBindings;

	final AtomicReferenceArray[] scopeCaches;

	private static Supplier<Function<CompiledBinding<?>, CompiledBinding<?>>> bytecodePostprocessorFactory = Function::identity;

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
			Function<CompiledBinding<?>, CompiledBinding<?>> specializer = (Function<CompiledBinding<?>, CompiledBinding<?>>) specializerInstance;
			Injector.bytecodePostprocessorFactory = () -> specializer;
		} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException | ClassNotFoundException | InstantiationException e) {
			throw new IllegalStateException("Can not access ActiveJ Specializer", e);
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
	 * This constructor combines given modules (along with a {@link DefaultModule})
	 * and then {@link #compile(Injector, Module) compiles} them.
	 */
	public static Injector of(Module... modules) {
		return compile(null, Modules.combine(Modules.combine(modules), new DefaultModule()));
	}

	public static Injector of(@Nullable Injector parent, Module... modules) {
		return compile(parent, Modules.combine(Modules.combine(modules), new DefaultModule()));
	}

	/**
	 * This constructor is a shortcut for threadsafe {@link #compile(Injector, Scope[], Trie, Multibinder, BindingTransformer, BindingGenerator) compile}
	 * with no instance overrides and no multibinders, transformers or generators.
	 */
	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return compile(null, UNSCOPED,
				bindings.map(map -> map.entrySet().stream().collect(toMap(Entry::getKey, entry -> new BindingSet<>(singleton(entry.getValue()), REGULAR)))),
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
	 * @param multibinder      a multibinder that is called on every binding conflict (see {@link Multibinder#combinedMultibinder})
	 * @param transformer      a transformer that is called on every binding once (see {@link BindingTransformer#combinedTransformer})
	 * @param generator        a generator that is called on every missing binding (see {@link BindingGenerator#combinedGenerator})
	 * @see #enterScope
	 */
	public static Injector compile(@Nullable Injector parent,
			Scope[] scope,
			@NotNull Trie<Scope, Map<Key<?>, BindingSet<?>>> bindingsMultimap,
			@NotNull Multibinder<?> multibinder,
			@NotNull BindingTransformer<?> transformer,
			@NotNull BindingGenerator<?> generator) {

		Trie<Scope, Map<Key<?>, MarkedBinding<?>>> bindings = Preprocessor.reduce(bindingsMultimap, multibinder, transformer, generator);

		Set<Key<?>> known = new HashSet<>();
		known.add(Key.of(Injector.class)); // injector is hardcoded in and will always be present
		if (parent != null) {
			known.addAll(parent.localCompiledBindings.keySet());
		}

		Trie<Scope, Map<Key<?>, Binding<?>>> justBindings = bindings.map(m -> m.entrySet().stream().collect(toMap(Entry::getKey, e -> e.getValue().getBinding())));
		Preprocessor.check(known, justBindings);

		Trie<Scope, ScopeLocalData> scopeDataTree = compileBindingsTrie(
				parent != null ? parent.scopeCaches.length : 0,
				scope,
				bindings,
				parent != null ? parent.localCompiledBindings : emptyMap()
		);
		return new Injector(parent, scopeDataTree);
	}

	protected static Trie<Scope, ScopeLocalData> compileBindingsTrie(int scope, Scope[] path,
			Trie<Scope, Map<Key<?>, MarkedBinding<?>>> bindings,
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
	protected static ScopeLocalData compileBindings(int scope, Scope[] path,
			Map<Key<?>, MarkedBinding<?>> bindings,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsOfParent
	) {
		Function<CompiledBinding<?>, CompiledBinding<?>> postprocessor = bytecodePostprocessorFactory.get();

		boolean threadsafe = path.length == 0 || path[path.length - 1].isThreadsafe();

		Map<Key<?>, CompiledBinding<?>> compiledBindings = new HashMap<>();
		compiledBindings.put(Key.of(Injector.class), postprocessor.apply(scope == 0 ?
				new CompiledBinding<Object>() {
					volatile Object instance;

					@Override
					public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
						Object instance = this.instance;
						if (instance != null) return instance;
						this.instance = scopedInstances[scope].get(0);
						return this.instance;
					}
				} :
				new CompiledBinding<Object>() {
					@Override
					public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
						return scopedInstances[scope].get(0);
					}
				}));

		Map<Key<?>, Integer> slotMapping = new HashMap<>();
		slotMapping.put(Key.of(Injector.class), 0);

		int[] nextSlot = {1};

		List<CompiledBinding<?>> eagerSingletons = new ArrayList<>();

		for (Entry<Key<?>, MarkedBinding<?>> entry : bindings.entrySet()) {
			Key<?> key = entry.getKey();
			MarkedBinding<?> binding = entry.getValue();
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

		bindings.put(Key.of(Injector.class), new MarkedBinding<>(Binding.to(() -> {
			throw new AssertionError("Injector constructor must never be called since it's instance is always put in the cache manually");
		}), EAGER));

		compiledBindingsOfParent.forEach(compiledBindings::putIfAbsent);

		int size = nextSlot[0];
		nextSlot[0] = -1;

		Map<Key<?>, BindingInfo> bindingInfo = bindings.entrySet().stream()
				.collect(toMap(Entry::getKey, e -> BindingInfo.from(e.getValue())));

		return new ScopeLocalData(path, bindingInfo, compiledBindings, slotMapping, size, eagerSingletons.toArray(new CompiledBinding[0]));
	}

	private static CompiledBinding<?> compileBinding(
			Function<CompiledBinding<?>, CompiledBinding<?>> postprocessor,
			int scope, Scope[] path, boolean threadsafe,
			Key<?> key, Map<Key<?>, MarkedBinding<?>> bindings,
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

		MarkedBinding<?> markedBinding = bindings.get(key);
		if (markedBinding == null) {
			CompiledBinding<?> compiled = compiledBindingsOfParent.getOrDefault(key, missingOptionalBinding());
			compiledBindings.put(key, compiled);
			return compiled;
		}

		Binding<?> binding = markedBinding.getBinding();
		BindingCompiler<?> compiler = binding.getCompiler();

		boolean plain = compiler instanceof PlainCompiler;

		Integer index;
		if (!plain && markedBinding.getType() != TRANSIENT) {
			slotMapping.put(key, index = nextSlot[0]++);
		} else {
			index = null;
		}

		CompiledBinding<?> compiled = postprocessor.apply(compiler.compile(
				new CompiledBindingLocator() {
					@SuppressWarnings("unchecked")
					@Override
					public @NotNull <Q> CompiledBinding<Q> get(Key<Q> key) {
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

		if (plain) {
			Key<?> target = ((PlainCompiler<?>) compiler).getKey();
			Integer targetIndex = slotMapping.get(target);
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
	@NotNull
	public <T> T getInstance(@NotNull Key<T> key) {
		CompiledBinding<?> binding = localCompiledBindings.get(key);
		if (binding == null) {
			throw DIException.cannotConstruct(key, null);
		}
		Object instance = binding.getInstance(scopeCaches, -1);
		if (instance == null) {
			throw DIException.cannotConstruct(key, scopeDataTree.get().bindingInfo.get(key));
		}
		return (T) instance;
	}

	/**
	 * @see #getInstance(Key)
	 */
	@Override
	@NotNull
	public <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.ofType(type));
	}

	/**
	 * Same as {@link #getInstance(Key)} except that it returns <code>null</code> instead of throwing an exception.
	 */
	@Override
	@SuppressWarnings("unchecked")
	@Nullable
	public <T> T getInstanceOrNull(@NotNull Key<T> key) {
		CompiledBinding<?> binding = localCompiledBindings.get(key);
		return binding != null ? (T) binding.getInstance(scopeCaches, -1) : null;
	}

	/**
	 * @see #getInstanceOrNull(Key)
	 */
	@Override
	@Nullable
	public <T> T getInstanceOrNull(@NotNull Class<T> type) {
		return getInstanceOrNull(Key.of(type));
	}

	/**
	 * Same as {@link #getInstanceOrNull(Key)}, but replaces <code>null</code> with given default value.
	 */
	@Override
	public <T> T getInstanceOr(@NotNull Key<T> key, T defaultValue) {
		T instance = getInstanceOrNull(key);
		return instance != null ? instance : defaultValue;
	}

	/**
	 * @see #getInstanceOr(Key, Object)
	 */
	@Override
	public <T> T getInstanceOr(@NotNull Class<T> type, T defaultValue) {
		return getInstanceOr(Key.of(type), defaultValue);
	}

	/**
	 * A shortcut for <code>getInstance(new Key&lt;InstanceProvider&lt;T&gt;&gt;(){})</code>
	 */
	@NotNull
	public <T> InstanceProvider<T> getInstanceProvider(@NotNull Key<T> key) {
		return getInstance(Key.ofType(Types.parameterized(InstanceProvider.class, key.getType()), key.getQualifier()));
	}

	/**
	 * @see #getInstanceProvider(Key)
	 */
	@NotNull
	public <T> InstanceProvider<T> getInstanceProvider(@NotNull Class<T> type) {
		return getInstanceProvider(Key.of(type));
	}

	/**
	 * A shortcut for <code>getInstance(new Key&lt;InstanceInjector&lt;T&gt;&gt;(){})</code>
	 */
	@NotNull
	public <T> InstanceInjector<T> getInstanceInjector(@NotNull Key<T> key) {
		return getInstance(Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getQualifier()));
	}

	/**
	 * @see #getInstanceInjector(Key)
	 */
	@NotNull
	public <T> InstanceInjector<T> getInstanceInjector(@NotNull Class<T> type) {
		return getInstanceInjector(Key.of(type));
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
	@Nullable
	@SuppressWarnings("unchecked")
	public <T> T peekInstance(@NotNull Key<T> key) {
		Integer index = localSlotMapping.get(key);
		return index != null ? (T) scopeCaches[scopeCaches.length - 1].get(index) : null;
	}

	/**
	 * @see #peekInstance(Key)
	 */
	@Nullable
	public <T> T peekInstance(@NotNull Class<T> type) {
		return peekInstance(Key.of(type));
	}

	/**
	 * This method checks if an instance for this key was created by a {@link #getInstance} call before.
	 */
	public boolean hasInstance(@NotNull Key<?> key) {
		return peekInstance(key) != null;
	}

	/**
	 * @see #hasInstance(Key)
	 */
	public boolean hasInstance(@NotNull Class<?> type) {
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
	 * @see #putInstance(Key, Object)
	 */
	public <T> void putInstance(Class<T> key, T instance) {
		putInstance(Key.of(key), instance);
	}

	@Nullable
	public BindingInfo getBinding(Class<?> type) {
		return getBinding(Key.of(type));
	}

	@Nullable
	public BindingInfo getBinding(Key<?> key) {
		return scopeDataTree.get().bindingInfo.get(key);
	}

	/**
	 * This method returns true if a binding was bound for given key.
	 */
	public boolean hasBinding(Key<?> key) {
		return scopeDataTree.get().bindingInfo.containsKey(key);
	}

	/**
	 * @see #hasBinding(Key)
	 */
	public boolean hasBinding(Class<?> type) {
		return hasBinding(Key.of(type));
	}

	/**
	 * Creates an injector that operates on a binding graph at a given prefix (scope) of the binding graph trie and this injector as its parent.
	 */
	public Injector enterScope(@NotNull Scope scope) {
		return new Injector(this, scopeDataTree.get(scope));
	}

	@Nullable
	public Injector getParent() {
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
	public Map<Key<?>, BindingInfo> getBindings() {
		return scopeDataTree.get().bindingInfo;
	}

	/**
	 * This method returns a trie of bindings, similar to {@link Module#getReducedBindingInfo()}
	 * <p>
	 * Note that this method expensive to call repeatedly
	 */
	public Trie<Scope, Map<Key<?>, BindingInfo>> getBindingsTrie() {
		return scopeDataTree.map(graph -> graph.bindingInfo);
	}

	@Override
	public String toString() {
		return "Injector{scope=" + getScopeDisplayString(scopeDataTree.get().scope) + '}';
	}
}
