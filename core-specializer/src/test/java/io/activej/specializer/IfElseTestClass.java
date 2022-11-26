package io.activej.specializer;

import org.jetbrains.annotations.Nullable;

public class IfElseTestClass implements TestInterface {
	private final @Nullable TestInterface delegate;

	public IfElseTestClass(@Nullable TestInterface delegate) {
		this.delegate = delegate;
	}

	@Override
	public int apply(int arg) {
		int result;

		if (delegate == null) {
			result = arg;
		} else {
			result = delegate.apply(arg);
		}

		return result;
	}
}
