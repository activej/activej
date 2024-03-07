package io.activej.http;

import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(Theories.class)
public class HttpHeadersMultimapTest {

	@DataPoints("initialSizes")
	public static int[] INITIAL_SIZES = new int[]{1, 2, 3, 4, 5, 10, 20, 100};

	@Theory
	public void testAddAndGet(@FromDataPoints("initialSizes") int initialSize) {
		HttpHeadersMultimap multimap = new HttpHeadersMultimap();
		int totalHeaders = initialSize * 10;

		for (int i = 0; i < totalHeaders; i++) {
			String iString = String.valueOf(i);
			HttpHeader key = HttpHeaders.of(iString);
			HttpHeaderValue value = HttpHeaderValue.of(iString);

			multimap.add(key, value);
		}

		for (int i = 0; i < totalHeaders; i++) {
			String iString = String.valueOf(i);
			HttpHeader key = HttpHeaders.of(iString);
			HttpHeaderValue value = multimap.get(key);
			assertNotNull(value);
			assertHttpHeaderValue(HttpHeaderValue.of(iString), value);
		}
	}

	private static void assertHttpHeaderValue(HttpHeaderValue expected, HttpHeaderValue actual) {
		assertSame(expected.getClass(), actual.getClass());
		assertArrayEquals(expected.getBuf().asArray(), actual.getBuf().asArray());
	}
}
