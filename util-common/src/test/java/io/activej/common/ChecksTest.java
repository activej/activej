package io.activej.common;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class ChecksTest {
	@Before
	public void setUp() {
		System.getProperties().stringPropertyNames().stream()
			.filter(s -> s.startsWith("chk"))
			.forEach(System::clearProperty);
		System.setProperty("chk", "on");
	}

	@Test
	public void testDisablingClass() {
		assertTrue(Checks.isEnabled(ChecksTest.class));

		System.setProperty("chk:" + ChecksTest.class.getName(), "off");
		assertFalse(Checks.isEnabled(ChecksTest.class));
	}

	@Test
	public void testDisablingClassBySimpleName() {
		assertTrue(Checks.isEnabled(ChecksTest.class));

		System.setProperty("chk:" + ChecksTest.class.getSimpleName(), "off");
		assertFalse(Checks.isEnabled(ChecksTest.class));
	}

	@Test
	public void testDisablingPackage() {
		assertTrue(Checks.isEnabled(List.class));

		System.setProperty("chk:java.util", "off");
		assertFalse(Checks.isEnabled(List.class));
		assertTrue(Checks.isEnabled(String.class)); // java.lang still enabled
	}

	@Test
	public void testDisablingPackageButEnablingClass() {
		assertTrue(Checks.isEnabled(List.class));
		assertTrue(Checks.isEnabled(ArrayList.class));

		System.setProperty("chk:java.util", "off");
		assertFalse(Checks.isEnabled(List.class));
		assertFalse(Checks.isEnabled(ArrayList.class));

		System.setProperty("chk:java.util.ArrayList", "on");
		assertFalse(Checks.isEnabled(List.class));
		assertTrue(Checks.isEnabled(ArrayList.class));
	}

	@Test
	public void testDisablingPackageButEnablingSubpackage() {
		assertTrue(Checks.isEnabled(List.class));
		assertTrue(Checks.isEnabled(Function.class));

		System.setProperty("chk:java.util", "off");
		assertFalse(Checks.isEnabled(List.class));
		assertFalse(Checks.isEnabled(Function.class));

		System.setProperty("chk:java.util.function", "on");
		assertFalse(Checks.isEnabled(List.class));
		assertTrue(Checks.isEnabled(Function.class));
	}

	@Test(expected = IllegalStateException.class)
	public void testAnonymousClass() {
		Checks.isEnabled(new Object() {}.getClass());
	}

}
