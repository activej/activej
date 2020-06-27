package io.activej.dataflow.dsl;

import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class EvaluationContext {
	private final List<String> prefixes = new ArrayList<>();
	private final Map<String, Dataset<?>> environment = new HashMap<>();

	private final DataflowContext context;

	public EvaluationContext(DataflowContext context) {
		this.context = context;
		prefixes.add("java.lang");
	}

	public void addPrefix(String prefix) {
		prefixes.add(prefix);
	}

	public void put(String name, Dataset<?> dataset) {
		environment.put(name, dataset);
	}

	public Dataset<?> get(String name) {
		return environment.get(name);
	}

	public DataflowContext getContext() {
		return context;
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> resolveClass(String s) {
		for (String prefix : prefixes) {
			try {
				if (prefix.endsWith("$")) {
					return (Class<T>) Class.forName(prefix + s);
				} else {
					return (Class<T>) Class.forName(prefix + "." + s);
				}
			} catch (ClassNotFoundException ignored) {
			}
		}
		try {
			return (Class<T>) Class.forName(s);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Failed to resolve class " + s);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T generateInstance(String s) {
		Class<Object> cls = resolveClass(s);
		try {
			Constructor<?> constructor = cls.getDeclaredConstructor();
			constructor.setAccessible(true);
			return (T) constructor.newInstance();
		} catch (ReflectiveOperationException e) {
			throw new IllegalArgumentException("Failed to generate an instance for class " + s);
		}
	}
}
