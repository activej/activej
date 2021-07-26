package io.activej.types;

import org.junit.Test;

import java.util.List;

import static io.activej.types.RecursiveType.of;
import static org.junit.Assert.assertEquals;

public class RecursiveTypeTest {

	@Test
	public void testClass() {
		assertEquals(Integer.class, of(Integer.class).getType());
	}

	@Test
	public void testListString() throws NoSuchFieldException {
		assertEquals(ListStringPojo.class.getField("list").getGenericType(),
				RecursiveType.of(List.class, of(String.class)).getType());
	}

	private static class ListStringPojo {
		public List<String> list;
		public List<? extends String> list2;
	}

	@Test
	public void testListExtendsString() throws NoSuchFieldException {
		assertEquals(RecursiveType.of(ListStringPojo.class.getField("list2").getGenericType()),
				RecursiveType.of(ListStringPojo.class.getField("list").getGenericType()));
	}

}
