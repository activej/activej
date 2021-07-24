package io.activej.common;

import io.activej.common.reflection.RecursiveType;
import io.activej.types.TypeT;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TypeTTest {

	@Test
	public void test1() {
		assertEquals(RecursiveType.of(List.class, RecursiveType.of(String.class)), RecursiveType.of(new TypeT<List<String>>() {}));
		assertEquals(RecursiveType.of(List.class, RecursiveType.of(String.class)), RecursiveType.of(new TypeT<List<? extends String>>() {}));
		assertEquals(RecursiveType.of(String.class), RecursiveType.of(new TypeT<String>() {}));
	}
}
