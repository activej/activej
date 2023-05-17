import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

/**
 * Example of serialization and deserialization of a simple object with no {@code null}
 * fields, generics or complex objects (such as maps or arrays) as fields.
 */
public final class SimpleObjectExample {
	public static void main(String[] args) {
		//[START REGION_2]
		Person jim = new Person(34, "Jim");
		jim.setSurname("Smith");
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
		System.out.println(jim.getSurname() + " " + johnCopy.getSurname());
		//[END REGION_4]
	}

	//[START REGION_1]
	public static class Person {
		public Person(@Deserialize("age") int age,
				@Deserialize("name") String name) {
			this.age = age;
			this.name = name;
		}

		@Serialize
		public final int age;

		@Serialize
		public final String name;

		private String surname;

		@Serialize
		public String getSurname() {
			return surname;
		}

		public void setSurname(String surname) {
			this.surname = surname;
		}
	}
	//[END REGION_1]
}
