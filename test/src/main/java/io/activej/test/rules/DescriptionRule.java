package io.activej.test.rules;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static io.activej.common.Checks.checkNotNull;

public final class DescriptionRule extends TestWatcher {
	private Description description;

	@Override
	protected void starting(Description description) {
		this.description = description;
	}

	public Description getDescription() {
		return checkNotNull(description);
	}
}
