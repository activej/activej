package io.activej.inject.impl;

import io.activej.inject.InstanceProvider;
import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.Scope;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.BindingToKey;
import io.activej.inject.binding.DIException;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.inject.binding.BindingType.SYNTHETIC;
import static io.activej.inject.util.Utils.*;

class BindingSynthesizer {
	private final Trie<Scope, ScopeState> scopeStateTrie;

	BindingSynthesizer() {
		this.scopeStateTrie = new Trie<>(new ScopeState(), new HashMap<>());
	}

	void addMissing(Scope[] scope, Key<?> key, boolean explicit) {
		ScopeState scopeState = scopeStateTrie.computeIfAbsent(scope, $ -> new ScopeState()).get();
		scopeState.missing().add(key, explicit);
	}

	void synthesizeMissing(Trie<Scope, Map<Key<?>, Binding<?>>> reduced) {
		synthesizeMissing(Scope.UNSCOPED, reduced);
	}

	private void synthesizeMissing(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> reduced) {
		Trie<Scope, ScopeState> trie = scopeStateTrie.get(scope);
		if (trie == null) return;

		ScopeState scopeState = trie.get();

		Scope[] currentScope = Scope.UNSCOPED;

		while (true) {
			Trie<Scope, Map<Key<?>, Binding<?>>> next = reduced.get(currentScope);
			synthesizeBindings(currentScope, scopeState, next.get());

			if (currentScope.length == scope.length) break;
			currentScope = next(currentScope, scope[currentScope.length]);
		}

		checkSynthesized(scope, scopeState);

		trie.getChildren().forEach((childScope, childTrie) -> synthesizeMissing(next(scope, childScope), reduced));

		Trie<Scope, Map<Key<?>, Binding<?>>> currentScopeTrie = reduced.get(scope);
		Map<Key<?>, Binding<?>> currentScopeBindings = currentScopeTrie.get();
		for (Map.Entry<Key<?>, Synthesized> entry : scopeState.synthesized().entrySet()) {
			Binding<?> binding = entry.getValue().binding();
			if (binding == null) continue;
			currentScopeBindings.put(entry.getKey(), binding);
		}
	}

	private void synthesizeBindings(Scope[] scope, ScopeState scopeState, Map<Key<?>, Binding<?>> localBindings) {
		for (Key<?> key : scopeState.missing.regular()) {
			Synthesized synthesized = synthesizeBinding(scope, scopeState, localBindings, key);
			if (synthesized != null) {
				scopeState.synthesized.put(key, synthesized);
			}
		}
		for (Key<?> key : scopeState.missing.instanceProviders()) {
			Synthesized synthesized = synthesizeInstanceProvider(scope, scopeState, localBindings, key);
			if (synthesized != null) {
				scopeState.synthesized.put(key, synthesized);
			}
		}
		for (Key<?> key : scopeState.missing.optionalDependencies()) {
			Synthesized synthesized = synthesizeOptionalDependency(scope, scopeState, localBindings, key);
			scopeState.synthesized.put(key, synthesized);
		}
	}

	private static @Nullable Synthesized synthesizeBinding(Scope[] scope, ScopeState scopeState, Map<Key<?>, Binding<?>> localBindings, Key<?> key) {
		Synthesized synthesized = scopeState.synthesized.get(key);
		if (synthesized != null) return synthesized;

		KeyPattern<?> pattern = KeyPattern.ofType(key.getType(), key.getQualifier());

		Map<Key<?>, Binding<?>> candidates = new HashMap<>();
		for (Map.Entry<Key<?>, Binding<?>> entry : localBindings.entrySet()) {
			Key<?> localKey = entry.getKey();
			if (!key.equals(localKey) && pattern.match(localKey)) {
				candidates.put(localKey, entry.getValue());
			}
		}

		return tryReduceCandidates(scope, candidates);
	}

	private static @Nullable Synthesized tryReduceCandidates(Scope[] scope, Map<Key<?>, Binding<?>> candidates) {
		Set<Key<?>> candidateKeys = candidates.keySet();
		Set<Key<?>> keysCopy = new HashSet<>(candidateKeys);
		for (Map.Entry<Key<?>, Binding<?>> entry : candidates.entrySet()) {
			if (entry.getValue() instanceof BindingToKey<?> bindingToKey && candidateKeys.contains(bindingToKey.getKey())) {
				keysCopy.remove(entry.getKey());
			}
		}

		if (keysCopy.isEmpty()) return null;

		if (keysCopy.size() == 1) {
			return new Synthesized(Binding.to(first(keysCopy)).as(SYNTHETIC));
		}

		return new Synthesized(new SynthesizeError(scope, keysCopy));
	}

	private static void checkSynthesized(Scope[] scope, ScopeState scopeState) {
		for (Key<?> key : scopeState.missing().all()) {
			Synthesized synthesized = scopeState.synthesized().get(key);
			if (synthesized == null) continue;

			SynthesizeError error = synthesized.error();
			if (error == null) continue;

			throw new DIException("Could not synthesize a binding for " + key.getDisplayString() +
								  " in scope " + getScopeDisplayString(scope) + ". " +
								  "Ambiguous bindings in scope " + getScopeDisplayString(error.scope()) +
								  ": " + error.ambiguousCandidates().stream().map(Key::getDisplayString).toList());
		}

		for (Key<?> key : scopeState.missing().explicit()) {
			Synthesized synthesized = scopeState.synthesized().get(key);
			if (synthesized != null) continue;

			throw new DIException("Could not synthesize explicit binding for " + key.getDisplayString() +
								  " in scope " + getScopeDisplayString(scope));
		}
	}

	private static Synthesized synthesizeOptionalDependency(Scope[] scope, ScopeState scopeState, Map<Key<?>, Binding<?>> localBindings, Key<?> key) {
		Key<?> instanceKey = key.getTypeParameter(0).qualified(key.getQualifier());

		Synthesized instanceSynthesized = synthesizeBinding(scope, scopeState, localBindings, instanceKey);

		if (instanceSynthesized == null || instanceSynthesized.binding() == null) {
			return new Synthesized(Binding.toInstance(OptionalDependency.empty()));
		}

		scopeState.synthesized.put(instanceKey, instanceSynthesized);

		return new Synthesized(optinalDependencyBinding(instanceKey));
	}

	private static Synthesized synthesizeInstanceProvider(Scope[] scope, ScopeState scopeState, Map<Key<?>, Binding<?>> localBindings, Key<?> key) {
		Key<Object> instanceKey = key.getTypeParameter(0).qualified(key.getQualifier());

		Synthesized instanceSynthesized = synthesizeBinding(scope, scopeState, localBindings, instanceKey);

		if (instanceSynthesized == null || instanceSynthesized.binding() == null) {
			return null;
		}

		scopeState.synthesized.put(instanceKey, instanceSynthesized);

		return new Synthesized(instanceProviderBinding(instanceKey));
	}

	static Binding<OptionalDependency<?>> optinalDependencyBinding(Key<?> instanceKey) {
		return new Binding<>(Set.of(instanceKey), SYNTHETIC, null) {
			@Override
			@SuppressWarnings({"rawtypes", "Convert2Lambda"})
			public CompiledBinding<OptionalDependency<?>> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return slot != null ?
					new AbstractCompiledBinding<>(scope, slot) {
						@Override
						protected OptionalDependency<?> doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							CompiledBinding<?> compiledBinding = compiledBindings.get(instanceKey);
							return OptionalDependency.of(compiledBinding.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new CompiledBinding<>() {
						@Override
						public OptionalDependency<?> getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							CompiledBinding<?> compiledBinding = compiledBindings.get(instanceKey);
							return OptionalDependency.of(compiledBinding.getInstance(scopedInstances, synchronizedScope));
						}
					};
			}
		};
	}

	static Binding<InstanceProvider<?>> instanceProviderBinding(Key<Object> instanceKey) {
		return new Binding<>(Set.of(instanceKey), SYNTHETIC, null) {
			@Override
			@SuppressWarnings({"rawtypes", "Convert2Lambda"})
			public CompiledBinding<InstanceProvider<?>> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return slot != null ?
					new AbstractCompiledBinding<>(scope, slot) {
						@Override
						protected InstanceProvider<?> doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							CompiledBinding<Object> compiledBinding = compiledBindings.get(instanceKey);
							// ^ this only gets already compiled binding, that's not a binding compilation after injector is compiled
							return new Preprocessor.InstanceProviderImpl<>(instanceKey, compiledBinding, scopedInstances, synchronizedScope);
						}
					} :
					new CompiledBinding<>() {
						@Override
						public InstanceProvider<?> getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {

							// transient bindings for instance provider are useless and nobody should make ones
							// however, things like mapInstance create an intermediate transient compiled bindings of their peers
							// usually they call getInstance just once and then cache the result of their computation (e.g. the result of mapping function)
							//
							// anyway all the above means that it's ok here to just get the compiled binding and to not care about caching it

							CompiledBinding<Object> compiledBinding = compiledBindings.get(instanceKey);
							return new Preprocessor.InstanceProviderImpl<>(instanceKey, compiledBinding, scopedInstances, synchronizedScope);
						}
					};
			}
		};
	}

	private record ScopeState(
		Missing missing,
		Map<Key<?>, Synthesized> synthesized
	) {
		private ScopeState() {
			this(
				new Missing(),
				new HashMap<>()
			);
		}

		private record Missing(
			Set<Key<?>> regular,
			Set<Key<?>> instanceProviders,
			Set<Key<?>> optionalDependencies,

			Set<Key<?>> explicit
		) {
			private Missing() {
				this(
					new HashSet<>(),
					new HashSet<>(),
					new HashSet<>(),
					new HashSet<>()
				);
			}

			void add(Key<?> key, boolean explicit) {
				Class<?> rawType = key.getRawType();
				if (rawType == InstanceProvider.class) {
					instanceProviders.add(key);
				} else if (rawType == OptionalDependency.class) {
					optionalDependencies.add(key);
				} else {
					regular.add(key);
				}
				if (explicit) {
					this.explicit.add(key);
				}
			}

			Iterable<Key<?>> all() {
				return new Iterable<>() {
					@NotNull
					@Override
					public Iterator<Key<?>> iterator() {
						return multiIterator(
							regular.iterator(),
							instanceProviders.iterator(),
							optionalDependencies.iterator()
						);
					}
				};
			}
		}
	}

	private record SynthesizeError(Scope[] scope, Set<Key<?>> ambiguousCandidates) {
	}

	private static final class Synthesized {
		private final Binding<?> binding;
		private final SynthesizeError error;

		private Synthesized(Binding<?> binding) {
			this.binding = binding;
			this.error = null;
		}

		private Synthesized(SynthesizeError error) {
			this.binding = null;
			this.error = error;
		}

		public Binding<?> binding() {return binding;}

		public SynthesizeError error() {return error;}
	}
}
