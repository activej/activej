package io.activej.common.builder;

import io.activej.common.initializer.WithInitializer;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;

public abstract class AbstractBuilder<B extends WithInitializer<B>, T> implements Builder<T>, WithInitializer<B> {
	private boolean built;

	public final boolean isBuilt() {
		return built;
	}

	protected static void checkNotBuilt(AbstractBuilder<?, ?> self) {
		checkState(!self.built);
	}

	@Override
	public final T build() {
		checkNotBuilt(this);
		built = true;
		T instance = doBuild();
		checkNotNull(instance);
		return instance;
	}

	protected abstract T doBuild();
}
