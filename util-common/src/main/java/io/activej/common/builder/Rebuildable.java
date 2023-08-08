package io.activej.common.builder;

public interface Rebuildable<T, B extends Builder<T>> {
	B rebuild();
}
