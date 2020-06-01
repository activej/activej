package io.activej.jmx.helper;

import io.activej.jmx.api.JmxBeanAdapter;

public final class JmxBeanAdapterStub implements JmxBeanAdapter {
	@Override
	public void execute(Object bean, Runnable command) {
		command.run();
	}
}
