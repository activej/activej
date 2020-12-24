import io.activej.bytebuf.ByteBuf;
import io.activej.codec.StructuredCodec;
import io.activej.codec.binary.BinaryUtils;
import io.activej.common.exception.MalformedDataException;
import util.Person;
import util.Registry;

import java.time.LocalDate;

//[START EXAMPLE]
public final class CodecStructuredBinaryExample {
	public static void main(String[] args) throws MalformedDataException {
		StructuredCodec<Person> PERSON_CODEC = Registry.REGISTRY.get(Person.class);

		Person john = new Person(121, "John", LocalDate.of(1990, 3, 12));
		System.out.println("Person before encoding: " + john);

		ByteBuf byteBuf = BinaryUtils.encode(PERSON_CODEC, john);
		Person decodedPerson = BinaryUtils.decode(PERSON_CODEC, byteBuf);
		System.out.println("Person after encoding: " + decodedPerson);
		System.out.println("Persons are equal? : " + john.equals(decodedPerson));
	}
}
//[END EXAMPLE]
