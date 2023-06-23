package io.activej.cube.http;

import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.StringConverter;
import io.activej.common.exception.MalformedDataException;
import io.activej.cube.bean.TestPubRequest.TestEnum;
import org.junit.Test;

import java.time.LocalDate;

import static io.activej.cube.Utils.CUBE_DSL_JSON;
import static io.activej.cube.Utils.fromJson;
import static org.junit.Assert.*;

public class JsonTest {

	@Test
	public void errorOnTrailingData() throws MalformedDataException {
		ReadObject<String> reader = StringConverter.READER;
		String stringJson = "\"string\"";
		String string = fromJson(reader, stringJson);
		assertEquals("string", string);

		String moreData = "  more data";
		try {
			fromJson(reader, stringJson + moreData);
			fail();
		} catch (MalformedDataException e){
			assertEquals(e.getMessage(), "Unexpected JSON data: " + moreData);
		}
	}

	@Test
	public void errorOnMalformedLocalDate() {
		ReadObject<LocalDate> reader = CUBE_DSL_JSON.tryFindReader(LocalDate.class);
		assertNotNull(reader);
		try {
			fromJson(reader, "\"INVALID DATE\"");
			fail();
		} catch (MalformedDataException e){
			assertEquals(e.getMessage(), "com.dslplatform.json.ParsingException: " +
					"Text 'INVALID DATE' could not be parsed at index 0. " +
					"Found \" at position: 14, following: `\"INVALID DATE\"`");
		}
	}

	@Test
	public void errorOnMalformedEnum() {
		ReadObject<TestEnum> reader = CUBE_DSL_JSON.tryFindReader(TestEnum.class);
		assertNotNull(reader);
		try {
			fromJson(reader, "\"INVALID ENUM\"");
			fail();
		} catch (MalformedDataException e){
			assertEquals(e.getMessage(), "com.dslplatform.json.ParsingException: " +
					"No enum constant io.activej.cube.bean.TestPubRequest.TestEnum.INVALID ENUM. " +
					"Found \" at position: 14, following: `\"INVALID ENUM\"`");
		}
	}
}
