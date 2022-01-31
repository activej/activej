package io.activej.inject.binding;

import io.activej.inject.Key;
import io.activej.inject.impl.CompiledBinding;
import io.activej.inject.impl.CompiledBindingLocator;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public final class BindingToKey<T> extends Binding<T> {
	private final Key<? extends T> key;

	BindingToKey(Key<? extends T> key) {
		super(Set.of(key));
		this.key = key;
	}

	public Key<? extends T> getKey() {
		return key;
	}

	@Override
	public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
		//noinspection unchecked
		return (CompiledBinding<T>) compiledBindings.get(key);
	}
}
