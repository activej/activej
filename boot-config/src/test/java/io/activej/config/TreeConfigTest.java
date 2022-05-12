package io.activej.config;

import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Set;

import static io.activej.config.ConfigTestUtils.testBaseConfig;
import static io.activej.config.converter.ConfigConverters.ofByte;
import static io.activej.config.converter.ConfigConverters.ofInteger;
import static org.junit.Assert.*;

public class TreeConfigTest {
	private Config config;

	@Before
	public void setUp() {
		config = Config.create()
				.with("key1", "value1")
				.with("key2.key3", Config.create().with("key4", "value4"))
				.with("key5.key6", "6");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCanNotAddBadPath() {
		config.with("a..b.", "illegalValue");
	}

	@Test
	public void testBase() {
		Config config = Config.create()
				.with("a.a.a", "1")
				.with("a.a.b", "2")
				.with("a.b", "3")
				.with("b", "4");
		testBaseConfig(config);
	}

	@Test
	public void testWorksProperlyWithChildren() {
		assertTrue(config.hasChild("key2.key3"));
		Config child = config.getChild("key2.key3");
		assertNotNull(child);
		assertEquals(Set.of("key4"), child.getChildren().keySet());
		assertEquals(Set.of("key1", "key2", "key5"), config.getChildren().keySet());

		assertEquals(
				config.getChild("key2.key3.key4"),
				config.getChild("key2").getChild("key3").getChild("key4")
		);
	}

	@Test
	public void testWorksWithDefaultValues() {
		Config root = Config.create();
		Integer value = root.get(ofInteger(), "not.existing.branch", 8080);
		assertEquals(8080, (int) value);
	}

	@Test(expected = NoSuchElementException.class)
	public void testShouldThrowNoSuchElementIfMissing() {
		config.get("a.b.c.d");
	}

	@Test
	public void testWorksProperlyWithConverters() {
		Byte value = config.get(ofByte(), "key5.key6");
		assertEquals((byte) 6, (byte) value);
	}
}
