package io.activej.config.converter;

import io.activej.common.MemSize;
import io.activej.config.Config;
import io.activej.eventloop.net.DatagramSocketSettings;
import io.activej.eventloop.net.ServerSocketSettings;
import io.activej.eventloop.net.SocketSettings;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.config.Config.THIS;
import static io.activej.config.ConfigTestUtils.assertIllegalArgument;
import static io.activej.config.ConfigTestUtils.assertNotPresent;
import static io.activej.config.converter.ConfigConverters.*;
import static org.junit.Assert.*;

public class ConfigConvertersTest {
	@Test
	public void testBaseGet() {
		Map<String, Config> map = new HashMap<>();
		map.put("key1", Config.ofValue("data1"));
		map.put("key2", Config.ofValue("data2"));
		map.put("key3", Config.ofValue("data3"));
		Config root = Config.ofConfigs(map);

		assertEquals("data1", root.get("key1"));
		assertEquals("data2", root.get("key2"));
		assertEquals("data3", root.get("key3"));
	}

	@Test
	public void testTransform() {
		Config test = Config.create()
				.with("durationToLong", "228 millis")
				.with("periodToInt", "1 days");

		// Basic test
		assertEquals(228L, (long) test.get(ofDuration().transform(Duration::toMillis, Duration::ofMillis), "durationToLong"));
		assertEquals(1, (int) test.get(ofPeriod().transform(Period::getDays, Period::ofDays), "periodToInt"));

		// Test of default value
		assertEquals((Long) 123L, test.get(ofDuration().transform(Duration::toMillis, Duration::ofMillis), "nonExistingPath", 123L));

		assertNotPresent(() -> test.get(ofDuration().transform(Duration::toMillis, Duration::ofMillis), "nonExistingPath"));
	}

	@Test
	public void testBooleanConverter() {
		Config config1 = Config.ofValue("true");
		Config config2 = Config.ofValue("false");

		assertTrue(config1.get(ofBoolean(), THIS));
		assertFalse(config2.get(ofBoolean(), THIS));
	}

	@Test
	public void testIntegerConverter() {
		Map<String, Config> values = new HashMap<>();
		values.put("key1", Config.ofValue("1"));
		values.put("key2", Config.ofValue("-5"));
		values.put("key3", Config.ofValue("100"));
		Config root = Config.ofConfigs(values);

		assertEquals(1, (int) root.get(ofInteger(), "key1"));
		assertEquals(-5, (int) root.get(ofInteger(), "key2"));
		assertEquals(100, (int) root.get(ofInteger(), "key3"));
	}

	@Test
	public void testLongConverter() {
		Map<String, Config> values = new HashMap<>();
		values.put("key1", Config.ofValue("1"));
		values.put("key2", Config.ofValue("-5"));
		values.put("key3", Config.ofValue("100"));
		Config root = Config.ofConfigs(values);

		assertEquals(1L, (long) root.get(ofLong(), "key1"));
		assertEquals(-5L, (long) root.get(ofLong(), "key2"));
		assertEquals(100L, (long) root.get(ofLong(), "key3"));
	}

	private enum Color {
		RED, GREEN, BLUE
	}

	@Test
	public void testEnumConverter() {
		ConfigConverter<Color> enumConverter = ConfigConverters.ofEnum(Color.class);
		Map<String, Config> values = new HashMap<>();
		values.put("key1", Config.ofValue("RED"));
		values.put("key2", Config.ofValue("GREEN"));
		values.put("key3", Config.ofValue("BLUE"));
		Config root = Config.ofConfigs(values);

		assertEquals(Color.RED, root.get(enumConverter, "key1"));
		assertEquals(Color.GREEN, root.get(enumConverter, "key2"));
		assertEquals(Color.BLUE, root.get(enumConverter, "key3"));
	}

	@Test
	public void testDoubleConverter() {
		ConfigConverter<Double> doubleConverter = ConfigConverters.ofDouble();
		Map<String, Config> map = new HashMap<>();
		map.put("key1", Config.ofValue("0.001"));
		map.put("key2", Config.ofValue("1e5"));
		map.put("key3", Config.ofValue("-23.1"));
		Config root = Config.ofConfigs(map);

		double acceptableError = 1e-10;
		assertEquals(0.001, doubleConverter.get(root.getChild("key1")), acceptableError);
		assertEquals(1e5, doubleConverter.get(root.getChild("key2")), acceptableError);
		assertEquals(-23.1, doubleConverter.get(root.getChild("key3")), acceptableError);
	}

	@Test
	public void testInetAddressConverter() throws UnknownHostException {
		ConfigConverter<InetSocketAddress> inetSocketAddressConverter = ConfigConverters.ofInetSocketAddress();
		Map<String, Config> map = new HashMap<>();
		map.put("key1", Config.ofValue("192.168.1.1:80"));
		map.put("key2", Config.ofValue("250.200.100.50:10000"));
		map.put("key3", Config.ofValue("1.0.0.0:65000"));
		map.put("key4", Config.ofValue("127.0.0.1:0"));

		Config root = Config.ofConfigs(map);

		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("192.168.1.1"), 80);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("250.200.100.50"), 10000);
		InetSocketAddress address3 = new InetSocketAddress(InetAddress.getByName("1.0.0.0"), 65000);
		InetSocketAddress address4 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
		assertEquals(address1, inetSocketAddressConverter.get(root.getChild("key1")));
		assertEquals(address2, inetSocketAddressConverter.get(root.getChild("key2")));
		assertEquals(address3, inetSocketAddressConverter.get(root.getChild("key3")));
		assertEquals(address4, inetSocketAddressConverter.get(root.getChild("key4")));
	}

	@Test
	public void testListConverter() {
		ConfigConverter<List<Integer>> listConverter = ConfigConverters.ofList(ConfigConverters.ofInteger(), ",");

		assertEquals(List.of(1, 5, 10), listConverter.get(Config.ofValue("1, 5,   10   ")));
		assertEquals(List.of(), listConverter.get(Config.ofValue("")));
		assertEquals(List.of(1, 2, 3), listConverter.get(Config.EMPTY, List.of(1, 2, 3)));
		assertNotPresent(() -> listConverter.get(Config.EMPTY));
	}

	@Test
	public void testDefaultNullValues() {
		Integer value = ofInteger().get(Config.EMPTY, null);
		assertNull(value);
	}

	@Test
	public void testDatagraphSocketSettingsConverter() {
		DatagramSocketSettings expected = DatagramSocketSettings.create()
				.withReceiveBufferSize(MemSize.bytes(256))
				.withSendBufferSize(MemSize.kilobytes(1))
				.withReuseAddress(false)
				.withBroadcast(true);

		DatagramSocketSettings actual = Config.EMPTY.get(ofDatagramSocketSettings(), THIS, expected);

		assertEquals(expected.getBroadcast(), actual.getBroadcast());
		assertEquals(expected.getReuseAddress(), actual.getReuseAddress());
		assertEquals(expected.getReceiveBufferSize(), actual.getReceiveBufferSize());
		assertEquals(expected.getSendBufferSize(), actual.getSendBufferSize());
	}

	@Test
	public void testServerSocketSettings() {
		ServerSocketSettings expected = ServerSocketSettings.create(1)
				.withReceiveBufferSize(MemSize.of(64))
				.withReuseAddress(true);

		ServerSocketSettings actual = Config.EMPTY.get(ofServerSocketSettings(), THIS, expected);
		assertEquals(expected.getBacklog(), actual.getBacklog());
		assertEquals(expected.getReceiveBufferSize(), actual.getReceiveBufferSize());
		assertEquals(expected.getReuseAddress(), actual.getReuseAddress());
	}

	@Test
	public void testSocketSettings() {
		SocketSettings expected = SocketSettings.create()
				.withTcpNoDelay(true)
				.withReuseAddress(false)
				.withReceiveBufferSize(MemSize.of(256))
				.withSendBufferSize(MemSize.of(512))
				.withKeepAlive(true);

		SocketSettings actual = Config.EMPTY.get(ofSocketSettings(), THIS, expected);

		assertFalse(actual.hasReadBufferSize());

		assertEquals(expected.getTcpNoDelay(), actual.getTcpNoDelay());
		assertEquals(expected.getReuseAddress(), actual.getReuseAddress());
		assertEquals(expected.getReceiveBufferSize(), actual.getReceiveBufferSize());
		assertEquals(expected.getSendBufferSize(), actual.getSendBufferSize());
		assertEquals(expected.getKeepAlive(), actual.getKeepAlive());
	}

	@Test
	public void testLocalDate() {
		LocalDate expected = LocalDate.of(1998, 12, 23);
		LocalDate actual = Config.EMPTY.get(ofLocalDate(), THIS, expected);

		assertEquals(expected, actual);
	}

	@Test
	public void testLocalTime() {
		LocalTime expected = LocalTime.of(6, 30, 28, 228);
		LocalTime actual = Config.EMPTY.get(ofLocalTime(), THIS, expected);

		assertEquals(expected, actual);
	}

	@Test
	public void testLocalDateTime() {
		LocalDateTime expected = LocalDateTime.of(LocalDate.of(1, 4, 28), LocalTime.of(2, 2, 8));
		LocalDateTime actual = Config.EMPTY.get(ofLocalDateTime(), THIS, expected);

		assertEquals(expected, actual);
	}

	@Test
	public void testPeriod() {
		Period expected = Period.of(1998, -12, 23);

		assertEquals(expected, Config.EMPTY.get(ofPeriod(), THIS, expected));
		assertEquals("1998 years -12 months 23 days", Config.ofValue(ofPeriod(), expected).getValue());
		assertEquals(Period.ZERO, Config.ofValue(ofPeriod(), Period.ZERO).get(ofPeriod(), THIS));
		assertEquals(expected, Config.ofValue(" -12 months 1998 year 23 days").get(ofPeriod(), THIS));

		assertIllegalArgument(() -> Config.ofValue(" 1 2 3 ").get(ofPeriod(), THIS));
		assertIllegalArgument(() -> Config.ofValue(" 1998 years 12 month 23 days 12 months").get(ofPeriod(), THIS));
		assertIllegalArgument(() -> Config.ofValue(" 1998 years12 month 23 days").get(ofPeriod(), THIS));
	}

	@Test
	public void testDuration() {
		Duration expected = Duration.ofDays(-2).plusHours(2).plusMinutes(8).plusSeconds(-3).plusMillis(-322).plusNanos(228);

		assertEquals(expected, Config.EMPTY.get(ofDuration(), THIS, expected));
		assertEquals("-1 days -21 hours -52 minutes -4 seconds 678 millis 228 nanos", Config.ofValue(ofDuration(), expected).getValue());
		assertEquals("0 seconds", Config.ofValue(ofDuration(), Duration.ZERO).getValue());
		assertEquals(Duration.ZERO, Config.ofValue(ofDuration(), Duration.ZERO).get(ofDuration(), THIS));
		assertEquals(expected, Config.ofValue("2 hour  -2 days 8 minutes 228 nanos -3 second -322 millis").get(ofDuration(), THIS));
		assertEquals(Duration.ofDays(2).plusHours(14).plusMinutes(1).plusSeconds(30), Config.ofValue("2.5 days 2 hour 1.5 minute").get(ofDuration(), THIS));
		assertEquals(Duration.ofHours(12).plusMinutes(40).plusSeconds(1).plusMillis(500), Config.ofValue("12.5 hours 10 minutes 1.5 second").get(ofDuration(), THIS));
		assertEquals(Duration.ofHours(2).plusSeconds(36), Config.ofValue("2.01 hours").get(ofDuration(), THIS));
		assertEquals(Duration.ofDays(2).plusMinutes(1).plusSeconds(26).plusMillis(400), Config.ofValue("2.001 days").get(ofDuration(), THIS));

		assertIllegalArgument(() -> Config.ofValue(" 1 2 3 ").get(ofDuration(), THIS));
		assertIllegalArgument(() -> Config.ofValue("1 days2 hours").get(ofDuration(), THIS));
		assertIllegalArgument(() -> Config.ofValue("1 day 2 hours 2 hours").get(ofDuration(), THIS));
		assertIllegalArgument(() -> Config.ofValue("2.2 nanos").get(ofDuration(), THIS));
	}

	@Test
	public void testInstant() {
		Instant expected = Instant.now();
		Instant actual = Config.EMPTY.get(ofInstant(), THIS, expected);

		assertEquals(expected, actual);
	}

	/**
	 * Testing *as* methods like ofDurationAsMillis, ofPeriodAsDays, etc...
	 */
	@Test
	public void testXAsY() {
		Instant now = Instant.now();
		Config testConfig = Config.create()
				.with("ofDurationAsMillis", "228 millis")
				.with("ofPeriodAsDays", "3 days")
				.with("ofMemSizeAsBytesLong", "2gb")
				.with("ofInstantAsEpochMillis", Config.ofValue(ofInstant(), now));

		assertEquals(228L, (long) testConfig.get(ofDurationAsMillis(), "ofDurationAsMillis"));
		assertEquals(3, (int) testConfig.get(ofPeriodAsDays(), "ofPeriodAsDays"));
		assertEquals(MemSize.gigabytes(2).toLong(), (long) testConfig.get(ofMemSizeAsLong(), "ofMemSizeAsBytesLong"));
		assertEquals(now.toEpochMilli(), (long) testConfig.get(ofInstantAsEpochMillis(), "ofInstantAsEpochMillis"));
	}

	@Test
	public void testStringConverters() {
		String stringValue = "test";
		String defaultValue = "default";

		String presentPath = "present";
		String emptyPath = "empty";
		String nonPresentPath = "nonpresent";

		Config config = Config.create()
				.with(emptyPath, "")
				.with(presentPath, stringValue);

		// no default
		assertEquals(stringValue, config.get(ofString(), presentPath));
		assertTrue(config.get(ofString(), emptyPath).isEmpty());
		assertNotPresent(() -> config.get(ofString(), nonPresentPath));

		// with default
		assertEquals(stringValue, config.get(ofString(), presentPath, defaultValue));
		assertTrue(config.get(ofString(), emptyPath, defaultValue).isEmpty());
		assertEquals(defaultValue, config.get(ofString(), nonPresentPath, defaultValue));
	}
}
