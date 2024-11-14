package io.activej.json;

import io.activej.common.exception.MalformedDataException;
import org.junit.Test;

import java.util.Map;

import static io.activej.json.JsonCodecs.*;
import static io.activej.json.JsonUtils.fromJson;
import static io.activej.json.JsonUtils.toJson;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JsonCodecTest {
	@Test
	public void testList() throws MalformedDataException {
		JsonCodec<Object[]> codec = ofArrayObject(ofString(), ofInteger());
		Object[] data = new Object[]{"abc", 123};
		String json = toJson(codec, data);
		assertArrayEquals(data, fromJson(codec, json));
	}

	@Test
	public void testObjectMap() throws MalformedDataException {
		JsonCodec<Map<String, ?>> codec = ofMapObject(Map.of(
			"a", ofString(),
			"b", ofInteger()));
		Map<String, ?> data = Map.of(
			"a", "abc",
			"b", 123);
		String json = toJson(codec, data);
		assertEquals(data, fromJson(codec, json));
	}

	public record Data(int x, String s) {}

	@Test
	public void testObject() throws MalformedDataException {
		var codec = ofObject(
			Data::new, "x", Data::x, ofInteger(),
			"b", Data::s, ofString()
		);
		var data = new Data(10, "abc");
		String json = toJson(codec, data);
		assertEquals(data, fromJson(codec, json));
	}
}
