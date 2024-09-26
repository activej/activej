package io.activej.cube;

import io.activej.common.exception.MalformedDataException;
import io.activej.cube.bean.TestPubRequest.TestEnum;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecFactory;
import io.activej.json.JsonCodecs;
import org.junit.Test;

import java.time.LocalDate;

import static io.activej.json.JsonUtils.fromJson;
import static org.junit.Assert.*;

public class JsonTest {

	@Test
	public void errorOnTrailingData() throws MalformedDataException {
		JsonCodec<String> reader = JsonCodecs.ofString();
		String stringJson = "\"string\"";
		String string = fromJson(reader, stringJson);
		assertEquals("string", string);

		String moreData = "  more data";
		MalformedDataException e = assertThrows(MalformedDataException.class, () -> fromJson(reader, stringJson + moreData));
		assertEquals(e.getMessage(), "Unexpected JSON data: " + moreData);
	}

	@Test
	public void errorOnMalformedLocalDate() {
		JsonCodec<LocalDate> reader = JsonCodecFactory.defaultInstance().resolve(LocalDate.class);
		assertNotNull(reader);
		MalformedDataException e = assertThrows(MalformedDataException.class, () -> fromJson(reader, "\"INVALID DATE\""));
		assertEquals(e.getMessage(), "com.dslplatform.json.ParsingException: " +
									 "Text 'INVALID DATE' could not be parsed at index 0. " +
									 "Found \" at position: 14, following: `\"INVALID DATE\"`");
	}

	@Test
	public void errorOnMalformedEnum() {
		JsonCodec<TestEnum> reader = JsonCodecFactory.defaultInstance().resolve(TestEnum.class);
		assertNotNull(reader);
		MalformedDataException e = assertThrows(MalformedDataException.class, () -> fromJson(reader, "\"INVALID ENUM\""));
		assertEquals(e.getMessage(), "com.dslplatform.json.ParsingException: " +
									 "No enum constant io.activej.cube.bean.TestPubRequest.TestEnum.INVALID ENUM. " +
									 "Found \" at position: 14, following: `\"INVALID ENUM\"`");
	}
}
