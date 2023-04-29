package io.activej.launchers.http;

import io.activej.config.Config;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.Promise.ofCompletionStage;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MultithreadedHttpServerLauncherTest {
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
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Provides
			@Worker
			AsyncServlet servlet(@WorkerId int worker) {
				throw new UnsupportedOperationException();
			}
		};
		launcher.testInjector();
	}

	@Test
	public void bindZeroPort() {
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Provides
			AsyncServlet servlet() {
				return req -> HttpResponse.ok200().toPromise();
			}

			@Override
			Config config() {
				return super.config()
						.overrideWith(Config.create().with("http.listenAddresses", "0"));
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
		assertEquals(0, launcher.primaryServer.getListenAddresses().get(0).getPort());
		assertNotEquals(0, launcher.primaryServer.getBoundAddresses().get(0).getPort());

		launcher.shutdown();
		await(ofCompletionStage(launcher.getCompleteFuture()));
	}

	@Test
	public void bindNonZeroPort() {
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Provides
			AsyncServlet servlet() {
				return req -> HttpResponse.ok200().toPromise();
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
		assertEquals(
				MultithreadedHttpServerLauncher.PORT,
				launcher.primaryServer.getListenAddresses().get(0).getPort());
		assertEquals(
				MultithreadedHttpServerLauncher.PORT,
				launcher.primaryServer.getBoundAddresses().get(0).getPort());

		launcher.shutdown();
		await(ofCompletionStage(launcher.getCompleteFuture()));
	}
}
