package util;

import io.activej.codec.StructuredCodec;
import io.activej.codec.registry.CodecRegistry;

import java.time.LocalDate;

import static io.activej.codec.StructuredCodecs.*;

//[START EXAMPLE]
public final class Registry {
	public static final CodecRegistry REGISTRY = CodecRegistry.create()
			.with(LocalDate.class, StructuredCodec.of(
					in -> LocalDate.parse(in.readString()),
					(out, item) -> out.writeString(item.toString())))
			.with(Person.class, registry -> object(Person::new,
					"id", Person::getId, INT_CODEC,
					"name", Person::getName, STRING_CODEC,
					"date of birth", Person::getDateOfBirth, registry.get(LocalDate.class)));
}
//[END EXAMPLE]
