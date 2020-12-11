import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredCodecs;
import io.activej.codec.json.JsonUtils;
import io.activej.common.exception.MalformedDataException;
import util.Person;
import util.Registry;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.codec.StructuredCodecs.INT_CODEC;
import static java.util.Arrays.asList;

//[START EXAMPLE]
public final class CodecStructuredCollectionsExample {
	private static final StructuredCodec<Person> PERSON_CODEC = Registry.REGISTRY.get(Person.class);
	private static final Person john = new Person(121, "John", LocalDate.of(1990, 3, 12));
	private static final Person sarah = new Person(124, "Sarah", LocalDate.of(1992, 6, 27));

	private static void encodeDecodeList() throws MalformedDataException {
		List<Person> persons = new ArrayList<>(asList(john, sarah));

		StructuredCodec<List<Person>> listCodec = StructuredCodecs.ofList(PERSON_CODEC);
		System.out.println("Persons before encoding: " + persons);

		String json = JsonUtils.toJson(listCodec, persons);
		System.out.println("List as json: " + json);

		List<Person> decodedPersons = JsonUtils.fromJson(listCodec, json);
		System.out.println("Persons after encoding: " + decodedPersons);
		System.out.println("Persons are equal? : " + persons.equals(decodedPersons));
		System.out.println();
	}

	private static void encodeDecodeMap() throws MalformedDataException {
		Map<Integer, Person> personsMap = new HashMap<>();
		personsMap.put(sarah.getId(), sarah);
		personsMap.put(john.getId(), john);

		StructuredCodec<Map<Integer, Person>> mapCodec = StructuredCodecs.ofMap(INT_CODEC, PERSON_CODEC);
		System.out.println("Map of persons before encoding: " + personsMap);

		String json = JsonUtils.toJson(mapCodec, personsMap);
		System.out.println("Map as json: " + json);

		Map<Integer, Person> decodedPersons = JsonUtils.fromJson(mapCodec, json);
		System.out.println("Map of persons after encoding: " + decodedPersons);
		System.out.println("Maps are equal? : " + personsMap.equals(decodedPersons));
	}

	public static void main(String[] args) throws MalformedDataException {
		encodeDecodeList();
		encodeDecodeMap();
	}
}
//[END EXAMPLE]
