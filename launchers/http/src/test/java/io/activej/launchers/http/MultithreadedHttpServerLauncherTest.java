package io.activej.launchers.http;

import io.activej.http.AsyncServlet;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.test.rules.ByteBufRule;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class MultithreadedHttpServerLauncherTest {

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
}
