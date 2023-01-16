package io.activej.common.initializer;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;

public abstract class AbstractBuilder<B extends WithInitializer<B>, T> implements Builder<T>, WithInitializer<B> {
	private volatile T instance;

	public final boolean isBuilt() {
		return instance != null;
	}

	protected static void checkNotBuilt(AbstractBuilder<?, ?> self) {
		checkState(!self.isBuilt());
	}

	@Override
	public final T build() {
		T t = instance;
		if (t != null) return t;
		return buildImpl();
	}

	synchronized private T buildImpl() {
		if (instance != null) return instance;
		instance = doBuild();
		checkNotNull(instance);
		return instance;
	}

	protected abstract T doBuild();
}
