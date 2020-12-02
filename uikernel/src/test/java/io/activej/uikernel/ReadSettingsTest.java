package io.activej.uikernel;

import com.google.gson.Gson;
import io.activej.common.exception.parse.ParseException;
import io.activej.http.HttpParseException;
import io.activej.http.HttpRequest;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class ReadSettingsTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testParseEncoded() throws HttpParseException, ParseException {
		String query = "fields=%5B%22name%22%2C%22surname%22%2C%22phone%22%2C%22age%22%2C%22gender%22%5D" +
				"&offset=0" +
				"&limit=10" +
				"&filters=%7B%22search%22%3A%22A%22%2C%22age%22%3A%225%22%2C%22gender%22%3A%22FEMALE%22%7D" +
				"&extra=%5B%5D";
		HttpRequest req = HttpRequest.get("http://127.0.0.1/?" + query);
		Gson gson = new Gson();
		ReadSettings<?> settings = ReadSettings.from(gson, req);
		assertTrue(settings.getExtra().isEmpty());
		assertEquals(10, settings.getLimit());
		assertEquals("A", settings.getFilters().get("search"));
		assertEquals(5, settings.getFields().size());
	}

	@Test
	public void testUtf8() throws Exception {
		String query = "fields=[first, second, third]" +
				"&offset=0" +
				"&limit=55" +
				"&filters={age:12, name:Арт%26уሴр}" + // added utf-8 symbol and encoded ampersand
				"&sort=[[name,asc]]" +
				"&extra=[]";

		HttpRequest req = HttpRequest.get("http://127.0.0.1/?" + query);
		Gson gson = new Gson();
		ReadSettings<?> settings = ReadSettings.from(gson, req);
		assertEquals("Арт&уሴр", settings.getFilters().get("name"));
	}
}
