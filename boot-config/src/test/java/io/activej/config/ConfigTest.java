package io.activej.config;

import io.activej.config.converter.ConfigConverters;
import io.activej.eventloop.net.ServerSocketSettings;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.activej.config.Config.THIS;
import static io.activej.config.ConfigTestUtils.assertIllegalArgument;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ConfigTest {
	@Test
	public void testTrim() {
		Map<String, Config> map = new HashMap<>();
		map.put("a", Config.ofValue(" value "));
		map.put("b", Config.ofValue("value "));
		Config config = Config.ofConfigs(map);
		assertEquals(" value ", config.get("a"));
		assertEquals("value ", config.get("b"));
	}

	@Test
	public void testOfConverter1() {
		Config config = Config.ofValue(ConfigConverters.ofServerSocketSettings(), ServerSocketSettings.create(16384));
		assertEquals("16384", config.get("backlog"));
		assertTrue(config.hasChild("receiveBufferSize"));
		assertEquals("", config.get("receiveBufferSize", "X"));
	}

	@Test
	public void testOfConverter2() {
		Config config = Config.ofValue(ConfigConverters.ofLong(), 0L);
		assertEquals("0", config.get(THIS));
	}

	@Test
	public void testCombine() {
		Config config1 = Config.EMPTY
				.with("a", "a")
				.with("a.a", "aa");
		Config config2 = Config.EMPTY
				.with("b", "b")
				.with("a.b", "ab");
		Config config = config1.combineWith(config2);
		assertEquals(Set.of("a", "b"), config.getChildren().keySet());
		assertEquals(Set.of("a", "b"), config.getChildren().get("a").getChildren().keySet());
		assertEquals("a", config.get("a"));
		assertEquals("b", config.get("b"));
		assertEquals("aa", config.get("a.a"));
		assertEquals("ab", config.get("a.b"));

		assertIllegalArgument(() -> config1.combineWith(Config.EMPTY.with("a", "x")));
		assertIllegalArgument(() -> config1.combineWith(Config.EMPTY.with("a.a", "x")));

		config1.combineWith(Config.EMPTY.with("a.a.a", "x"));
	}
}
