package io.activej.async.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LogUtilsTest {

	@Test
	public void thisMethod() {
		assertEquals("methodA", methodA());
		assertEquals("methodB", methodB());
		assertEquals("methodA", methodC());
	}

	private static String methodA() {
		return LogUtils.thisMethod();
	}

	private static String methodB() {
		return LogUtils.thisMethod();
	}

	private static String methodC() {
		return methodA();
	}
}
