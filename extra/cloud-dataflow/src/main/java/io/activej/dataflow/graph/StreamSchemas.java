package io.activej.dataflow.graph;

import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.serializer.BinarySerializer;

public final class StreamSchemas {
	public static <T> StreamSchema_Simple<T> simple(Class<T> cls) {
		return new StreamSchema_Simple<>(cls);
	}

	public static class StreamSchema_Simple<T> implements StreamSchema<T> {
		private final Class<T> cls;

		private StreamSchema_Simple(Class<T> cls) {
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
