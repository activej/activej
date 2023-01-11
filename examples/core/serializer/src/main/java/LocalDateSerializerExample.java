import io.activej.serializer.*;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.time.LocalDate;
import java.util.Arrays;

/**
 * An example of writing a {@link SerializerDef} for a LocalDate and attaching it to a {@link SerializerBuilder}
 * <p>
 * This example can be used as a guideline of how to write custom serializers for arbitrary classes
 * <p>
 * To serialize a LocalDate, we need to serialize 3 int values:
 * {@link LocalDate#getYear()}, {@link LocalDate#getMonth()}, {@link LocalDate#getDayOfMonth()}.
 * We can do it by writing 3 varInts(compact ints) in a sequence
 * <p>
 * To deserialize a LocalDate, we need to read 3 varInts and construct a new {@link LocalDate} by calling
 * {@link LocalDate#of(int, int, int)}
 */
public final class LocalDateSerializerExample {
	public static void main(String[] args) {
		//[START SERIALIZER_CREATE]
		BinarySerializer<LocalDateHolder> serializer =
				SerializerBuilder.create()
						.with(LocalDate.class, ctx -> new SerializerDef_LocalDate())
						.build(LocalDateHolder.class);
		//[END SERIALIZER_CREATE]

		byte[] array = new byte[1024];

		LocalDateHolder localDateHolder = new LocalDateHolder(LocalDate.now());

		System.out.println("Serializing LocalDateHolder: " + localDateHolder);

		int newPos = serializer.encode(array, 0, localDateHolder);

		System.out.println("Byte array with serialized LocalDateHolder: " + Arrays.toString(Arrays.copyOf(array, newPos)));

		LocalDateHolder decoded = serializer.decode(array, 0);

		System.out.println("Deserialized LocalDateHolder: " + decoded);
	}

	//[START HOLDER]
	public static class LocalDateHolder {
		@Serialize
		public final LocalDate date;

		public LocalDateHolder(@Deserialize("date") LocalDate date) {
			this.date = date;
		}

		@Override
		public String toString() {
			return "LocalDateHolder{date=" + date + '}';
		}
	}
	//[END HOLDER]

	//[START SERIALIZER]
	public static class SerializerDef_LocalDate extends SimpleSerializerDef<LocalDate> {
		@Override
		protected BinarySerializer<LocalDate> createSerializer(int version, CompatibilityLevel compatibilityLevel) {
			return new BinarySerializer<>() {
				@Override
				public void encode(BinaryOutput out, LocalDate localDate) {
					out.writeVarInt(localDate.getYear());
					out.writeVarInt(localDate.getMonthValue());
					out.writeVarInt(localDate.getDayOfMonth());
				}

				@Override
				public LocalDate decode(BinaryInput in) throws CorruptedDataException {
					int year = in.readVarInt();
					int month = in.readVarInt();
					int day = in.readVarInt();

					return LocalDate.of(year, month, day);
				}
			};
		}
	}
	//[END SERIALIZER]
}
