package io.activej.launchers.http;

import io.activej.di.annotation.Provides;
import io.activej.http.AsyncServlet;
import io.activej.test.rules.ByteBufRule;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;
import org.junit.ClassRule;
import org.junit.Test;

public class MultithreadedHttpServerLauncherTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

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
