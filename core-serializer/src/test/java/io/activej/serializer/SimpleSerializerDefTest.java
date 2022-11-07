package io.activej.serializer;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import org.junit.Test;

import static io.activej.serializer.Utils.doTest;
import static org.junit.Assert.assertEquals;

public class SimpleSerializerDefTest {

	@Test
	public void test() {
		BinarySerializer<ExternalClass> serializer = SerializerBuilder.create()
				.with(ExternalComponent.class, ctx -> new SimpleSerializerDef<ExternalComponent>() {
					@Override
					protected BinarySerializer<ExternalComponent> createSerializer(int version, CompatibilityLevel compatibilityLevel) {
						return new BinarySerializer<ExternalComponent>() {
							@Override
							public void encode(BinaryOutput out, ExternalComponent item) {
								out.writeVarInt(item.getX());
								out.writeUTF8(item.getY());
							}

							@Override
							public ExternalComponent decode(BinaryInput in) throws CorruptedDataException {
								int x = in.readVarInt();
								String y = in.readUTF8();

								return new ExternalComponent(x, y);
							}
						};
					}
				})
				.build(ExternalClass.class);

		ExternalClass original = new ExternalClass(
				"test",
				new ExternalComponent(
						123,
						"inner test 1"
				),
				new ExternalComponent(
						456,
						"inner test 2"
				)
		);

		ExternalClass copy = doTest(original, serializer);

		assertEquals(original, copy);
	}

	public static final class ExternalClass {
		private final String name;
		private final ExternalComponent component1;
		private final ExternalComponent component2;

		public ExternalClass(@Deserialize("name") String name, @Deserialize("component1") ExternalComponent component1, @Deserialize("component2") ExternalComponent component2) {
			this.name = name;
			this.component1 = component1;
			this.component2 = component2;
		}

		@Serialize
		public String getName() {
			return name;
		}

		@Serialize
		public ExternalComponent getComponent1() {
			return component1;
		}

		@Serialize
		public ExternalComponent getComponent2() {
			return component2;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ExternalClass that = (ExternalClass) o;

			if (!name.equals(that.name)) return false;
			if (!component1.equals(that.component1)) return false;
			return component2.equals(that.component2);
		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + component1.hashCode();
			result = 31 * result + component2.hashCode();
			return result;
		}
	}

	public static final class ExternalComponent {
		private final int x;
		private final String y;

		public ExternalComponent(int x, String y) {
			this.x = x;
			this.y = y;
		}

		public int getX() {
			return x;
		}

		public String getY() {
			return y;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ExternalComponent that = (ExternalComponent) o;

			if (x != that.x) return false;
			return y.equals(that.y);
		}

		@Override
		public int hashCode() {
			int result = x;
			result = 31 * result + y.hashCode();
			return result;
		}
	}
}
