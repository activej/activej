package io.activej.http;

import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.time.Month.JANUARY;
import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class HttpDateTest {

	@Test
	public void testRender() {
		LocalDate date = LocalDate.of(2015, JANUARY, 1);
		byte[] bytes = new byte[29];
		int end = HttpDate.render(date.atStartOfDay().toInstant(UTC).getEpochSecond(), bytes, 0);
		assertEquals(29, end);
		assertArrayEquals("Thu, 01 Jan 2015 00:00:00 GMT".getBytes(ISO_8859_1), bytes);
	}

	@Test
	public void testParser() throws MalformedHttpException {
		String date = "Thu, 01 Jan 2015 02:00:00 GMT";
		long actual = HttpDate.parse(date.getBytes(ISO_8859_1), 0);

		LocalDateTime dateTime = LocalDateTime.of(2015, JANUARY, 1, 2, 0);
		assertEquals(actual, dateTime.toInstant(UTC).getEpochSecond());
	}

	@Test
	public void testFull() throws MalformedHttpException {
		byte[] bytes = new byte[29];
		HttpDate.render(4073580000L, bytes, 0);
		assertEquals(4073580000L, HttpDate.parse(bytes, 0));
	}

	@Test
	public void testDateWithShortYear() throws MalformedHttpException {
		String date = "Thu, 01 Jan 15 00:00:00 GMT";
		long actual = HttpDate.parse(date.getBytes(ISO_8859_1), 0);

		LocalDate expected = LocalDate.of(2015, JANUARY, 1);
		assertEquals(expected.atStartOfDay().toInstant(UTC).getEpochSecond(), actual);
	}
}
