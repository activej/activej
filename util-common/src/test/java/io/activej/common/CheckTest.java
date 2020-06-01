package io.activej.common;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class CheckTest {
	@Before
	public void setUp() {
		System.getProperties().stringPropertyNames().stream()
				.filter(s -> s.startsWith("chk"))
				.forEach(System::clearProperty);
	}

	@Test
	public void testDisablingClass() {
		assertTrue(Check.isEnabled(CheckTest.class));

		System.setProperty("chk:" + CheckTest.class.getName(), "off");
		assertFalse(Check.isEnabled(CheckTest.class));
	}

	@Test
	public void testDisablingClassBySimpleName() {
		assertTrue(Check.isEnabled(CheckTest.class));

		System.setProperty("chk:" + CheckTest.class.getSimpleName(), "off");
		assertFalse(Check.isEnabled(CheckTest.class));
	}

	@Test
	public void testDisablingPackage() {
		assertTrue(Check.isEnabled(List.class));

		System.setProperty("chk:java.util", "off");
		assertFalse(Check.isEnabled(List.class));
		assertTrue(Check.isEnabled(String.class)); // java.lang still enabled
	}

	@Test
	public void testDisablingPackageButEnablingClass() {
		assertTrue(Check.isEnabled(List.class));
		assertTrue(Check.isEnabled(ArrayList.class));

		System.setProperty("chk:java.util", "off");
		assertFalse(Check.isEnabled(List.class));
		assertFalse(Check.isEnabled(ArrayList.class));

		System.setProperty("chk:java.util.ArrayList", "on");
		assertFalse(Check.isEnabled(List.class));
		assertTrue(Check.isEnabled(ArrayList.class));
	}

	@Test
	public void testDisablingPackageButEnablingSubpackage() {
		assertTrue(Check.isEnabled(List.class));
		assertTrue(Check.isEnabled(Function.class));

		System.setProperty("chk:java.util", "off");
		assertFalse(Check.isEnabled(List.class));
		assertFalse(Check.isEnabled(Function.class));

		System.setProperty("chk:java.util.function", "on");
		assertFalse(Check.isEnabled(List.class));
		assertTrue(Check.isEnabled(Function.class));
	}

}
