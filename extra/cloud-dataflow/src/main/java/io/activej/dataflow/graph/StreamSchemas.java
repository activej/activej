package io.activej.dataflow.graph;

import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.serializer.BinarySerializer;

public final class StreamSchemas {
	public static <T> SimpleStreamSchema<T> simple(Class<T> cls) {
		return new SimpleStreamSchema<>(cls);
	}

	public static class SimpleStreamSchema<T> implements StreamSchema<T> {
		private final Class<T> cls;

		private SimpleStreamSchema(Class<T> cls) {
			this.cls = cls;
		}

		@Override
		public Class<T> createClass() {
			return cls;
		}

		@Override
		public BinarySerializer<T> createSerializer(BinarySerializerModule.BinarySerializerLocator locator) {
			return locator.get(cls);
		}

		public Class<?> getCls() {
			return cls;
		}

		@Override
		public String toString() {
			return "Simple{" +
					"cls=" + cls +
					'}';
		}
	}
}
