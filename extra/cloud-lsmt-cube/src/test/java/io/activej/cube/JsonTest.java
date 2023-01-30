package io.activej.cube;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.StringConverter;
import io.activej.common.exception.MalformedDataException;
import org.junit.Test;

import static io.activej.cube.Utils.fromJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonTest {

	@Test
	public void errorOnTrailingData() throws MalformedDataException {
		JsonReader.ReadObject<String> reader = StringConverter.READER;
		String stringJson = "\"string\"";
		String string = fromJson(reader, stringJson);
		assertEquals("string", string);

		String moreData = "  more data";
		try {
			fromJson(reader, stringJson + moreData);
			fail();
		} catch (MalformedDataException e) {
			assertEquals(e.getMessage(), "Unexpected JSON data: " + moreData);
		}
	}
}
