package io.activej.launchers.http;

import io.activej.config.Config;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.Promise.ofCompletionStage;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class HttpServerLauncherTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@BeforeClass
	public static void beforeClass() {
		Injector.useSpecializer();
	}

	@Test
	public void testsInjector() {
		HttpServerLauncher launcher = new HttpServerLauncher() {
			@Provides
			public AsyncServlet servlet() {
				throw new UnsupportedOperationException();
			}
		};
		launcher.testInjector();
	}

	@Test
	public void bindZeroPort() {
		HttpServerLauncher launcher = new HttpServerLauncher() {
			@Provides
			AsyncServlet servlet() {
				return req -> HttpResponse.ok200();
			}

			@Override
			Config config() {
				return super.config().with("http.listenAddresses", "0");
			}
		};

		new Thread(() -> {
			try {
				launcher.launch(Launcher.NO_ARGS);
			} catch (Exception e) {
				throw new AssertionError(e);
			}
		}).start();

		await(ofCompletionStage(launcher.getStartFuture()));
		assertEquals(1, launcher.httpServer.getListenAddresses().size());
		assertEquals(0, launcher.httpServer.getListenAddresses().get(0).getPort());
		assertEquals(1, launcher.httpServer.getBoundAddresses().size());
		assertNotEquals(0, launcher.httpServer.getBoundAddresses().get(0).getPort());

		launcher.shutdown();
		await(ofCompletionStage(launcher.getCompleteFuture()));
	}

	@Test
	public void bindNonZeroPort() {
		HttpServerLauncher launcher = new HttpServerLauncher() {
			@Provides
			AsyncServlet servlet() {
				return req -> HttpResponse.ok200();
			}
		};

		new Thread(() -> {
			try {
				launcher.launch(Launcher.NO_ARGS);
			} catch (Exception e) {
				throw new AssertionError(e);
			}
		}).start();

		await(ofCompletionStage(launcher.getStartFuture()));
		assertEquals(1, launcher.httpServer.getListenAddresses().size());
		assertEquals(
				HttpServerLauncher.PORT,
				launcher.httpServer.getListenAddresses().get(0).getPort());
		assertEquals(1, launcher.httpServer.getBoundAddresses().size());
		assertEquals(
				HttpServerLauncher.PORT,
				launcher.httpServer.getBoundAddresses().get(0).getPort());

		launcher.shutdown();
		await(ofCompletionStage(launcher.getCompleteFuture()));
	}
}
