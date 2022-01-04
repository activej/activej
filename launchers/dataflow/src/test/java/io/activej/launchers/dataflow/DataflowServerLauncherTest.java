package io.activej.launchers.dataflow;

import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

public class DataflowServerLauncherTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testsInjector() {
		DataflowServerLauncher launcher = new DataflowServerLauncher() {};
		launcher.testInjector();
	}
}
