package io.activej.eventloop;

import io.activej.common.ref.Ref;
import io.activej.eventloop.inspector.EventloopStats;
import org.junit.Test;

import java.time.Duration;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

public final class EventloopTest {
	@Test
	public void testContextInFatalErrorHandlers() {
		StringBuilder sb = new StringBuilder();
		Ref<Throwable> errorRef = new Ref<>();
		Eventloop eventloop = Eventloop.builder()
			.withCurrentThread()
			.withFatalErrorHandler((e, context) -> {
				errorRef.set(e);
				sb.append(requireNonNull(context));
			})
			.build();
		RuntimeException error = new RuntimeException("error");
		String contextString = "Failed component";
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				throw error;
			}

			@Override
			public String toString() {
				return contextString;
			}
		});
		eventloop.run();
		assertEquals(error, errorRef.get());
		assertEquals(contextString, sb.toString());
	}

	@Test
	public void testGetSmoothingWindow() {
		Duration smoothingWindow = Eventloop.builder()
			.withInspector(EventloopStats.create())
			.build()
			.getSmoothingWindow();
		assertEquals(Eventloop.DEFAULT_SMOOTHING_WINDOW, smoothingWindow);
	}
}
