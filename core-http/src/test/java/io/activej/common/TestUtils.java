package io.activej.common;

import io.activej.http.HttpRequest;
import org.junit.Test;

import static io.activej.http.HttpUtils.getFullUri;
import static io.activej.http.HttpUtils.isInetAddress;
import static org.junit.Assert.*;

public class TestUtils {
	@Test
	public void testIsIpv4InetAddress() {
		String ip = "127.0.0.1";
		assertTrue(isInetAddress(ip));

		ip = ".127.0.0";
		assertFalse(isInetAddress(ip));

		ip = "255.255.255.255";
		assertTrue(isInetAddress(ip));

		ip = "0.0.0.0";
		assertTrue(isInetAddress(ip));

		ip = "345.213.2344.78568";
		assertFalse(isInetAddress(ip));

		ip = "11.11..11";
		assertFalse(isInetAddress(ip));

		ip = "11.";
		assertFalse(isInetAddress(ip));
	}

	@Test
	public void testIsIpv6InetAddress() {
		String ip = "FEDC:BA98:7654:3210:FEDC:BA98:7654:3210";
		assertTrue(isInetAddress(ip));

		ip = "f:0:e:0:A:0:C:0";
		assertTrue(isInetAddress(ip));

		ip = "::3210";
		assertTrue(isInetAddress(ip));

		ip = "::EEEE::3210";
		assertFalse(isInetAddress(ip));

		ip = "AAAAAAAA::3210";
		assertFalse(isInetAddress(ip));

		ip = "[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]";
		assertTrue(isInetAddress(ip));

		ip = "FEDC:BA98:7654:3210:FEDC:BA98:7654:3210:FEDC:BA98:7654:3210:FEDC:BA98:7654:3210";
		assertFalse(isInetAddress(ip));

		ip = "111...dfff:eeaa:";
		assertFalse(isInetAddress(ip));

		ip = "FEDC:BA98:";
		assertFalse(isInetAddress(ip));

		ip = "::127.0.0.1";
		assertTrue(isInetAddress(ip));

		ip = "0:0:0:0:0:0:13.1.68.3";
		assertTrue(isInetAddress(ip));
	}

	@Test
	public void testFullUriHttp() {
		String fullUri = "http://localhost:8080/example/test?param=1&param=2#test";
		HttpRequest httpRequest = HttpRequest.get(fullUri).build();
		assertEquals(getFullUri(httpRequest), fullUri);
	}

	@Test
	public void testFullUriHttps() {
		String fullUri = "https://localhost:8080/";
		HttpRequest httpRequest = HttpRequest.get(fullUri).build();
		assertEquals(fullUri, getFullUri(httpRequest));
	}

	@Test
	public void testFullUriWithoutSlash() {
		String fullUri = "https://localhost:8080";
		HttpRequest httpRequest = HttpRequest.get(fullUri).build();
		assertEquals(fullUri + "/", getFullUri(httpRequest));
	}

	@Test
	public void testFullUriWithoutPath() {
		String fullUri = "https://localhost:8080?test=test&test2=test2";
		HttpRequest httpRequest = HttpRequest.get(fullUri).build();
		assertEquals("https://localhost:8080" + "/" + "?test=test&test2=test2", getFullUri(httpRequest));
	}

	@Test
	public void testFullUriWithSlashWithoutPath() {
		String fullUri = "https://localhost:8080/?test=test&test2=test2";
		HttpRequest httpRequest = HttpRequest.get(fullUri).build();
		assertEquals(fullUri, getFullUri(httpRequest));
	}

	/**
	 * The authority component is terminated by the next slash ("/"), question mark ("?"),
	 * or number sign ("#") character, or by the end of the URI
	 * <a href="https://tools.ietf.org/html/rfc3986#section-3.2">...</a>
	 */
	@Test
	public void test() {
		String fullUri = "https://localhost:8080#####################";
		HttpRequest httpRequest = HttpRequest.get(fullUri).build();
		assertEquals("https://localhost:8080/#####################", getFullUri(httpRequest));
	}
}
