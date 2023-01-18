package io.activej.config;

import org.junit.Test;

import static io.activej.config.ConfigTestUtils.testBaseConfig;

public class ConfigBuilderTest {

	@Test
	public void testWith() {
		Config with = Config.builder()
				.with("a.a.a", Config.ofValue("1"))
				.with("a.a", Config.builder().with("b", Config.ofValue("2")).build())
				.with("a.b", Config.ofValue("3"))
				.with("b", Config.ofValue("4"))
				.build();
		testBaseConfig(with);
	}

	@Test
	public void testUnion() {
		Config config = Config.builder()
				.with("a.a.a", Config.ofValue("1"))
				.with("a.a", Config.builder().with("b", Config.ofValue("not:2")).build())
				.with("a.b", Config.ofValue("3"))
				.with("b", Config.ofValue("not:4"))
				.build();

		Config override = Config.builder()
				.with("a.a.b", Config.ofValue("2"))
				.with("b", Config.ofValue("4"))
				.build();

		testBaseConfig(config.overrideWith(override));
	}
}
