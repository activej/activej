package io.activej.types;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TypesTest {
	@Test
	public void testGetSimpleName() {
		assertEquals("String", Types.getSimpleName(String.class));
		assertEquals("List<String>", Types.getSimpleName(new TypeT<List<String>>() {}.getType()));
		assertEquals("List<?>", Types.getSimpleName(new TypeT<List<?>>() {}.getType()));
		assertEquals("List<? extends String>", Types.getSimpleName(new TypeT<List<? extends String>>() {}.getType()));
		//noinspection TypeParameterExplicitlyExtendsObject
		assertEquals("List<?>", Types.getSimpleName(new TypeT<List<? extends Object>>() {}.getType()));
		assertEquals("List<? super String>", Types.getSimpleName(new TypeT<List<? super String>>() {}.getType()));
		assertEquals("List<? super Object>", Types.getSimpleName(new TypeT<List<? super Object>>() {}.getType()));
		assertEquals("List<?>[]", Types.getSimpleName(new TypeT<List<?>[]>() {}.getType()));
	}
}
