package io.activej.config;

import io.activej.config.converter.ConfigConverter;
import io.activej.config.converter.ConfigConverters;
import org.junit.Assert;

import java.util.NoSuchElementException;

import static io.activej.common.Utils.setOf;
import static io.activej.config.Config.EMPTY;
import static org.junit.Assert.*;

@SuppressWarnings("WeakerAccess")
public class ConfigTestUtils {
	/**
	 * Config of the following form is required:
	 * key: a.a.a, value: 1;
	 * key: a.a.b, value: 2;
	 * key: a.b  , value: 3;
	 * key: b    , value: 4;
	 */
	public static void testBaseConfig(Config config) {
		testHasChild(config);
		testGetChild(config);
		testGetChildren(config);

		testHasValue(config);
		testSimpleGet(config);
		testSimpleGetWithDefault(config);
		testGetWithConverter(config);
		testGetWithConverterWithDefault(config);
	}

	public static void assertNotPresent(Runnable runnable) {
		assertError(runnable, NoSuchElementException.class);
	}

	public static void assertIllegalArgument(Runnable runnable) {
		assertError(runnable, IllegalArgumentException.class);
	}

	private static void assertError(Runnable runnable, Class<?> errorClass) {
		try {
			runnable.run();
			Assert.fail();
		} catch (Throwable e) {
			if (errorClass.isAssignableFrom(e.getClass())) return;
			throw e;
		}
	}

	private static void testHasChild(Config config) {
		assertTrue(config.hasChild("a"));
		assertTrue(config.hasChild("a.a"));
		assertTrue(config.hasChild("a.a.a"));
		assertTrue(config.hasChild("a.a.b"));
		assertTrue(config.hasChild("a.b"));
		assertTrue(config.hasChild("b"));

		assertTrue(config.hasChild(""));
		assertFalse(config.hasChild("a.a.c"));
		assertFalse(config.hasChild("a.c"));
		assertFalse(config.hasChild("c"));
	}

	private static void testGetChild(Config config) {
		assertNotNull(config.getChild("a.a.a"));
		assertNotNull(config.getChild("a.a.b"));
		assertNotNull(config.getChild("a.a"));
		assertNotNull(config.getChild("a.b"));
		assertNotNull(config.getChild("a"));
		assertNotNull(config.getChild("b"));

		assertEquals(EMPTY, config.getChild("a.a.c"));
		assertEquals(EMPTY, config.getChild("a.c"));
		assertEquals(EMPTY, config.getChild("c"));

		assertEquals(config.getChild(""), config);
		assertEquals(config.getChild("a").getChild("a").getChild("a"), config.getChild("a.a.a"));
		assertEquals(config.getChild("a").getChild("a").getChild("b"), config.getChild("a.a.b"));
		assertEquals(config.getChild("a").getChild("b"), config.getChild("a.b"));

		assertNotEquals(config.getChild("a").getChild("a").getChild("a"), config.getChild("a.a.b"));
	}

	private static void testGetChildren(Config config) {
		assertEquals(setOf("a", "b"), config.getChildren().keySet());
		assertEquals(setOf("a", "b"), config.getChild("a").getChildren().keySet());
		assertEquals(setOf("a", "b"), config.getChild("a.a").getChildren().keySet());

		assertTrue(config.getChild("a.a.a").getChildren().isEmpty());
	}

	private static void testHasValue(Config config) {
		assertTrue(config.getChild("a.a.a").hasValue());
		assertTrue(config.getChild("a.a.b").hasValue());
		assertTrue(config.getChild("a.b").hasValue());
		assertTrue(config.getChild("b").hasValue());

		// existing 'branch' routes
		assertFalse(config.getChild("a.a").hasValue());
		assertFalse(config.getChild("a").hasValue());
		assertFalse(config.hasValue());

		// not existing routes
		assertFalse(config.getChild("b.a.a").hasValue());
		assertFalse(config.getChild("a.a.c").hasValue());
	}

	private static void testSimpleGet(Config config) {
		assertEquals("1", config.get("a.a.a"));
		assertEquals("2", config.get("a.a.b"));
		assertEquals("3", config.get("a.b"));
		assertEquals("4", config.get("b"));

		testBadPaths(config);
	}

	private static void testSimpleGetWithDefault(Config config) {
		String defaultValue = "defaultValue";
		assertEquals("1", config.get("a.a.a", defaultValue));
		assertEquals("2", config.get("a.a.b", defaultValue));
		assertEquals("3", config.get("a.b", defaultValue));
		assertEquals("4", config.get("b", defaultValue));

		assertEquals(defaultValue, config.get("", defaultValue));
		assertEquals(defaultValue, config.get("a", defaultValue));
	}

	private static void testGetWithConverter(Config config) {
		ConfigConverter<Byte> converter = ConfigConverters.ofByte();

		assertEquals(1, (byte) config.get(converter, "a.a.a"));
		assertEquals(2, (byte) config.get(converter, "a.a.b"));
		assertEquals(3, (byte) config.get(converter, "a.b"));
		assertEquals(4, (byte) config.get(converter, "b"));

		testBadPaths(config);
	}

	private static void testGetWithConverterWithDefault(Config config) {
		ConfigConverter<Byte> converter = ConfigConverters.ofByte();
		byte defaultValue = 5;

		assertEquals(1, (byte) config.get(converter, "a.a.a", defaultValue));
		assertEquals(2, (byte) config.get(converter, "a.a.b", defaultValue));
		assertEquals(3, (byte) config.get(converter, "a.b", defaultValue));
		assertEquals(4, (byte) config.get(converter, "b", defaultValue));

		assertEquals(defaultValue, (byte) config.get(converter, "", defaultValue));
		assertEquals(defaultValue, (byte) config.get(converter, "a", defaultValue));
	}

	private static void testBadPaths(Config config) {
		try {
			config.get("");
			fail("should have no value for path: \"\"");
		} catch (NoSuchElementException ignored) {
		}

		try {
			config.get(".");
			fail("should have no value for path: \".\"");
		} catch (IllegalArgumentException ignored) {
		}

		try {
			config.get("a");
			fail("should have no value for path: \"a\"");
		} catch (NoSuchElementException ignored) {
		}

		try {
			config.get("a.a..");
			fail("should have no value for path \"a.a..\"");
		} catch (IllegalArgumentException ignored) {
		}
	}
}
