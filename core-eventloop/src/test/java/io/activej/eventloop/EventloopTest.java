package io.activej.eventloop;

import io.activej.common.ref.Ref;
import org.junit.Test;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

public final class EventloopTest {
	@Test
	public void testContextInFatalErrorHandlers() {
		StringBuilder sb = new StringBuilder();
		Ref<Throwable> errorRef = new Ref<>();
		Eventloop eventloop = Eventloop.create().withCurrentThread()
				.withFatalErrorHandler((e, context) -> {
					errorRef.set(e);
					sb.append(requireNonNull(context).toString());
				});
		RuntimeException error = new RuntimeException("error");
		String contextString = "Failed component";
		Object context = new Object() {
			@Override
			public String toString() {
				return contextString;
			}
		};
		eventloop.post(RunnableWithContext.of(context, () -> {
			throw error;
		}));
		eventloop.run();
		assertEquals(error, errorRef.get());
		assertEquals(contextString, sb.toString());
	}
}
