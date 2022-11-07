package io.activej.launcher;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.DIException;
import org.junit.Test;

import static org.junit.Assert.*;

public class LauncherTest {

	@Test
	public void injectErrorLogging() throws Exception {
		Exception testException = new Exception("Test");

		Launcher launcher = new Launcher() {
			@Provides
			@Eager
			int x() throws Exception {
				throw testException;
			}

			@Override
			protected void run() {
			}
		};

		Logger logger = (Logger) launcher.logger;
		ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
		listAppender.start();

		try {
			logger.addAppender(listAppender);
			launcher.launch(new String[0]);
			fail();
		} catch (DIException e) {
			assertSame(testException, e.getCause());
		} finally {
			logger.detachAppender(listAppender);
		}

		ILoggingEvent lastEvent = listAppender.list.get(listAppender.list.size() - 1);

		assertEquals(Level.ERROR, lastEvent.getLevel());
		assertEquals("Launch error", lastEvent.getMessage());
		assertSame(testException, ((ThrowableProxy) lastEvent.getThrowableProxy().getCause()).getThrowable());
	}
}
