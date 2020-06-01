import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeFixedSize;
import io.activej.serializer.annotations.SerializeNullable;

import java.util.Arrays;

import static java.lang.ClassLoader.getSystemClassLoader;

/**
 * Example of serialization and deserialization of an object with fixed size and nullable fields.
 */
public final class FixedSizeFieldsExample {
	//[START REGION_1]
	public static class Storage {
		@Serialize(order = 0)
		@SerializeFixedSize(3)
		@SerializeNullable(path = {0})
		public String[] strings;

		@Serialize(order = 1)
		@SerializeFixedSize(4)
		public byte[] bytes;
	}
	//[END REGION_1]


	public static void main(String[] args) {
		//[START REGION_2]
		Storage storage = new Storage();
		storage.strings = new String[]{"abc", null, "123", "superfluous"};
		storage.bytes = new byte[]{1, 2, 3, 4};

		byte[] buffer = new byte[200];
		BinarySerializer<Storage> serializer = SerializerBuilder.create(getSystemClassLoader())
				.build(Storage.class);
		//[END REGION_2]

		//[START REGION_3]
		serializer.encode(buffer, 0, storage);
		Storage limitedStorage = serializer.decode(buffer, 0);
		//[END REGION_3]

		//[START REGION_4]
		System.out.println(Arrays.toString(storage.strings) + " -> " + Arrays.toString(limitedStorage.strings));
		System.out.println(Arrays.toString(storage.bytes) + " -> " + Arrays.toString(limitedStorage.bytes));
		//[END REGION_4]
	}
}
