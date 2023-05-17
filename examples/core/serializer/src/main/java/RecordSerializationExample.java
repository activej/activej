import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.annotations.SerializeRecord;

/**
 * Example of serialization and deserialization of a Java record
 */
public final class RecordSerializationExample {
	public static void main(String[] args) {
		//[START REGION_2]
		Person jim = new Person(34, "Jim");
		byte[] buffer = new byte[200];
		BinarySerializer<Person> serializer = SerializerFactory.defaultInstance()
				.create(Person.class);
		//[END REGION_2]

		//[START REGION_3]
		serializer.encode(buffer, 0, jim);
		Person johnCopy = serializer.decode(buffer, 0);
		//[END REGION_3]

		//[START REGION_4]
		System.out.println(jim.age + " " + johnCopy.age);
		System.out.println(jim.name + " " + johnCopy.name);
		//[END REGION_4]
	}

	//[START REGION_1]
	@SerializeRecord
	public record Person(int age, String name) {
	}
	//[END REGION_1]
}
