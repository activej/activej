package io.activej.config;

import org.junit.Test;

import static io.activej.config.ConfigTestUtils.testBaseConfig;

public class ConfigBuilderTest {

	@Test
	public void testWith() {
		Config with = Config.create()
			.with("a.a.a", Config.ofValue("1"))
			.with("a.a", Config.create().with("b", Config.ofValue("2")))
			.with("a.b", Config.ofValue("3"))
			.with("b", Config.ofValue("4"));
		testBaseConfig(with);
	}

	@Test
	public void testUnion() {
		Config config = Config.create()
			.with("a.a.a", Config.ofValue("1"))
			.with("a.a", Config.create().with("b", Config.ofValue("not:2")))
			.with("a.b", Config.ofValue("3"))
			.with("b", Config.ofValue("not:4"));

		Config override = Config.create()
			.with("a.a.b", Config.ofValue("2"))
			.with("b", Config.ofValue("4"));

		testBaseConfig(config.overrideWith(override));
	}
}
