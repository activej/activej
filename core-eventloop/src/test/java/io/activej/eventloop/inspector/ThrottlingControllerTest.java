package io.activej.eventloop.inspector;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class ThrottlingControllerTest {

	private static final double DELTA = 1e-10;

	@Test
	public void testGcExceeding() {
		int gcMillis = 100;
		ThrottlingController throttlingController = ThrottlingController.create()
				.withGcTime(Duration.ofMillis(gcMillis));

		// normal execution
		throttlingController.onUpdateBusinessLogicTime(true, true, gcMillis / 2);
		throttlingController.onUpdateSelectorSelectTime(10);
		assertFalse(throttlingController.isOverloaded());
		assertEquals(0.0, throttlingController.getThrottling(), DELTA);

		// gc exceeded first time, ignore it
		throttlingController.onUpdateBusinessLogicTime(true, true, gcMillis * 2);
		throttlingController.onUpdateSelectorSelectTime(10);
		assertFalse(throttlingController.isOverloaded());
		assertEquals(0.0, throttlingController.getThrottling(), DELTA);

		// gc exceeded second time in a raw, maximum throttle
		throttlingController.onUpdateBusinessLogicTime(true, true, gcMillis * 2);
		throttlingController.onUpdateSelectorSelectTime(10);
		assertTrue(throttlingController.isOverloaded());
		assertEquals(1.0, throttlingController.getThrottling(), DELTA);

		assertEquals(1, throttlingController.getInfoGcExceeds());
	}
}
