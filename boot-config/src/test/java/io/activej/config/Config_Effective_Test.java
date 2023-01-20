package io.activej.config;

import io.activej.config.converter.ConfigConverter;
import io.activej.reactor.net.ServerSocketSettings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.config.ConfigTestUtils.testBaseConfig;
import static io.activej.config.converter.ConfigConverters.ofLong;
import static io.activej.config.converter.ConfigConverters.ofServerSocketSettings;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.*;

public class Config_Effective_Test {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Config_Effective config;

	@Before
	public void setUp() {
		Map<String, Config> db = new LinkedHashMap<>();
		db.put("jdbcUrl", Config.ofValue("jdbc:mysql://localhost:3306/some_schema"));
		db.put("username", Config.ofValue("root"));
		db.put("password", Config.ofValue("root"));
		Config dbConfig = Config.ofConfigs(db);

		Map<String, Config> server = new LinkedHashMap<>();
		server.put("port", Config.ofValue("8080"));
		server.put("businessTimeout", Config.ofValue("100ms"));
		Map<String, Config> asyncClient = new LinkedHashMap<>();
		asyncClient.put("clientTimeout", Config.ofValue("1000"));
		server.put("AsyncClient", Config.ofConfigs(asyncClient));
		Config serverConfig = Config.ofConfigs(server);
		Map<String, Config> root = new LinkedHashMap<>();

		root.put("DataBase", dbConfig);
		root.put("Server", serverConfig);

		config = Config_Effective.wrap(Config.ofConfigs(root));
	}

	@Test
	public void testBase() {
		Map<String, Config> tier3 = new HashMap<>();
		tier3.put("a", Config.ofValue("1"));
		tier3.put("b", Config.ofValue("2"));
		Map<String, Config> tier2 = new HashMap<>();
		tier2.put("a", Config.ofConfigs(tier3));
		tier2.put("b", Config.ofValue("3"));
		Map<String, Config> tier1 = new HashMap<>();
		tier1.put("a", Config.ofConfigs(tier2));
		tier1.put("b", Config.ofValue("4"));
		Config config = Config.ofConfigs(tier1);

		testBaseConfig(config);
	}

	@Test
	public void testCompoundConfig() {
		config = Config_Effective.wrap(
				Config.create()
						.with("Server.socketSettings.backlog", "10")
						.with("Server.socketSettings.receiveBufferSize", "10")
						.with("Server.socketSettings.reuseAddress", "true")
		);

		ConfigConverter<ServerSocketSettings> converter = ofServerSocketSettings();

		ServerSocketSettings settings = config.get(converter, "Server.socketSettings");
		settings = config.get(converter, "Server.socketSettings", settings);

		assertEquals(10, settings.getBacklog());
		assertEquals(10, settings.getReceiveBufferSize().toInt());
		assertTrue(settings.getReuseAddress());
	}

	@Test
	public void testWorksWithDefaultNulls() {
		String expected = """
				# a.a.a =\s
				## a.a.b = value1
				a.a.c = value2
				## a.b.a = value3
				""";

		Config_Effective config = Config_Effective.wrap(
				Config.create()
						.with("a.a.b", "value1")
						.with("a.a.c", "value2")
						.with("a.b.a", "value3")
		);

		assertNull(config.get("a.a.a", null));
		assertNotNull(config.get("a.a.c", null));

		String actual = config.renderEffectiveConfig();
		assertEquals(expected, actual);
	}

	@Test
	public void testEffectiveConfig() throws IOException {
		assertEquals("jdbc:mysql://localhost:3306/some_schema", config.get("DataBase.jdbcUrl"));
		assertEquals("root", config.get("DataBase.password"));
		assertEquals("root", config.get("DataBase.password", "default"));

		assertEquals(1000L, (long) config.get(ofLong(), "Server.AsyncClient.clientTimeout"));
		Path outputPath = temporaryFolder.newFile("./effective.properties").toPath();
		config.saveEffectiveConfigTo(outputPath);
		assertTrue(Files.exists(outputPath));
	}
}
