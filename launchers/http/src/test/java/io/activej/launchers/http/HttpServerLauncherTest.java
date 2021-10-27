package io.activej.launchers.http;

import io.activej.http.AsyncServlet;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.test.rules.ByteBufRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class HttpServerLauncherTest {
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
}
