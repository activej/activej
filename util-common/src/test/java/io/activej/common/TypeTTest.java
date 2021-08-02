package io.activej.common;

import io.activej.types.TypeT;
import org.junit.Test;

import java.util.List;

import static io.activej.types.Types.parameterizedType;
import static io.activej.types.Types.wildcardTypeExtends;
import static org.junit.Assert.assertEquals;

public class TypeTTest {

	@Test
	public void test1() {
		assertEquals(parameterizedType(List.class, String.class), new TypeT<List<String>>() {}.getType());
		assertEquals(parameterizedType(List.class, wildcardTypeExtends(String.class)), new TypeT<List<? extends String>>() {}.getType());
		assertEquals(String.class, new TypeT<String>() {}.getType());
	}
}
