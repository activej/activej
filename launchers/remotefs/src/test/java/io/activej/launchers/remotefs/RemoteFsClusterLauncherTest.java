package io.activej.launchers.remotefs;

import io.activej.config.Config;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.di.module.Module;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.test.TestUtils.getFreePort;

@Ignore
public final class RemoteFsClusterLauncherTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private int serverPort;

	@Before
	public void setUp() {
		serverPort = getFreePort();
	}

	@Test
	public void testInjector() {
		new RemoteFsClusterLauncher() {}.testInjector();
	}

	@Test
	@Ignore("startup point for the testing launcher override")
	public void launchServer() throws Exception {
		new RemoteFsServerLauncher() {
			@Override
			protected Module getOverrideModule() {
				return new AbstractModule() {
					@Provides
					Config config() {
						return Config.create()
								.with("remotefs.path", Config.ofValue("storages/server_" + serverPort))
								.with("remotefs.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(serverPort)));
					}
				};
			}

		}.launch(new String[0]);
	}

	@Test
	@Ignore("manual startup point for the testing launcher override")
	public void launchCluster() throws Exception {
		long start = System.nanoTime();
		createFiles(Paths.get("storages/local"), 1000, 10 * 1024, 100 * 1024);
		System.out.println("Created local files in " + ((System.nanoTime() - start) / 1e6) + " ms");

		new RemoteFsClusterLauncher() {
			@Override
			protected Module getOverrideModule() {
				return new AbstractModule() {
					@Provides
					Config config() {
						Config config = Config.create()
								.with("local.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8000)))
								.with("local.path", Config.ofValue("storages/local"))
								.with("cluster.replicationCount", Config.ofValue("3"))
								.with("scheduler.repartition.disabled", "true");
						for (int i = 0; i < 10; i++) {
							config = config.with("cluster.partitions.server_" + i, "localhost:" + (5400 + i));
						}
						return config;
					}
				};
			}
		}.launch(new String[0]);
	}

	private static void createFiles(Path path, int n, int minSize, int maxSize) throws IOException {
		Files.createDirectories(path);
		int delta = maxSize - minSize;
		Random rng = new Random(7L);
		for (int i = 0; i < n; i++) {
			byte[] data = new byte[minSize + (delta <= 0 ? 0 : rng.nextInt(delta))];
			rng.nextBytes(data);
			Files.write(path.resolve("file_" + i + ".txt"), data);
		}
	}
}
