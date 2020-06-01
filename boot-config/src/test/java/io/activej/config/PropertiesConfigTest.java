package io.activej.config;

import org.junit.Test;

import java.util.Properties;

import static io.activej.config.ConfigTestUtils.testBaseConfig;

public class PropertiesConfigTest {
	@Test
	public void testBase() {
		Properties properties = new Properties();
		properties.put("a.a.a", "1");
		properties.put("a.a.b", "2");
		properties.put("a.b", "3");
		properties.put("b", "4");

		testBaseConfig(Config.ofProperties(properties));
	}
}
