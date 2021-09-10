package io.activej.types;

import io.activej.types.Types.GenericArrayTypeImpl;
import io.activej.types.scanner.TestClass1;
import io.activej.types.scanner.TestClass2;
import org.junit.Test;

import java.util.*;

import static io.activej.types.IsAssignableUtils.isAssignable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("All") // too many 'redundant'-warnings
public class IsAssignableTest {

	@Test
	public void test1() {
		{
			List<String> to = null;
			List<String> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<String>>() {}.getType(),
					new TypeT<List<String>>() {}.getType()
			));
		}

		{
			List<CharSequence> to = null;
			List<String> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<CharSequence>>() {}.getType(),
					new TypeT<List<String>>() {}.getType()
			));
		}

		{
			List<String> to = null;
			List<CharSequence> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<String>>() {}.getType(),
					new TypeT<List<CharSequence>>() {}.getType()
			));
		}
	}

	@Test
	public void test2() {
		{
			List<?> to = null;
			List<String> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<?>>() {}.getType(),
					new TypeT<List<String>>() {}.getType()
			));
		}

		{
			List<? extends CharSequence> to = null;
			List<String> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? extends CharSequence>>() {}.getType(),
					new TypeT<List<String>>() {}.getType()
			));
		}

		{
			List<? extends String> to = null;
			List<CharSequence> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? extends String>>() {}.getType(),
					new TypeT<List<CharSequence>>() {}.getType()
			));
		}
	}

	@Test
	public void test3() {
		{
			List<? super Object> to = null;
			List<Object> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? super Object>>() {}.getType(),
					new TypeT<List<Object>>() {}.getType()
			));
		}

		{
			List<? super String> to = null;
			List<Object> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? super String>>() {}.getType(),
					new TypeT<List<Object>>() {}.getType()
			));
		}

		{
			List<? super String> to = null;
			List<CharSequence> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? super String>>() {}.getType(),
					new TypeT<List<CharSequence>>() {}.getType()
			));
		}

		{
			List<? super CharSequence> to = null;
			List<String> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? super CharSequence>>() {}.getType(),
					new TypeT<List<String>>() {}.getType()
			));
		}
	}

	@Test
	public void test4() {
		{
			List<CharSequence> to = null;
			List<? extends CharSequence> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<CharSequence>>() {}.getType(),
					new TypeT<List<? extends CharSequence>>() {}.getType()
			));
		}

		{
			List<CharSequence> to = null;
			List<? super CharSequence> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<CharSequence>>() {}.getType(),
					new TypeT<List<? super CharSequence>>() {}.getType()
			));
		}
	}

	@Test
	public void test5() {
		{
			Object to = null;
			CharSequence from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<Object>() {}.getType(),
					new TypeT<CharSequence>() {}.getType()
			));
		}

		{
			CharSequence to = null;
			Object from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<CharSequence>() {}.getType(),
					new TypeT<Object>() {}.getType()
			));
		}

		{
			List<?> to = null;
			List<? extends CharSequence> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<?>>() {}.getType(),
					new TypeT<List<? extends CharSequence>>() {}.getType()
			));
		}

		{
			List<? extends CharSequence> to = null;
			List<? extends CharSequence> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? extends CharSequence>>() {}.getType(),
					new TypeT<List<? extends CharSequence>>() {}.getType()
			));
		}

		{
			List<? extends CharSequence> to = null;
			List<? extends String> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? extends CharSequence>>() {}.getType(),
					new TypeT<List<? extends String>>() {}.getType()
			));
		}

		{
			List<? extends String> to = null;
			List<? extends CharSequence> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? extends String>>() {}.getType(),
					new TypeT<List<? extends CharSequence>>() {}.getType()
			));
		}

		{
			List<? extends CharSequence> to = null;
			List<? super CharSequence> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? extends CharSequence>>() {}.getType(),
					new TypeT<List<? super CharSequence>>() {}.getType()
			));
		}

		{
			List<? extends CharSequence> to = null;
			List<? super String> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? extends CharSequence>>() {}.getType(),
					new TypeT<List<? super String>>() {}.getType()
			));
		}

		{
			List<? extends String> to = null;
			List<? super CharSequence> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? extends String>>() {}.getType(),
					new TypeT<List<? super CharSequence>>() {}.getType()
			));
		}

		{
			List<? super CharSequence> to = null;
			List<? super CharSequence> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? super CharSequence>>() {}.getType(),
					new TypeT<List<? super CharSequence>>() {}.getType()
			));
		}

		{
			List<? super CharSequence> to = null;
			List<? super String> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? super CharSequence>>() {}.getType(),
					new TypeT<List<? super String>>() {}.getType()
			));
		}

		{
			List<? super String> to = null;
			List<? super CharSequence> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List<? super String>>() {}.getType(),
					new TypeT<List<? super CharSequence>>() {}.getType()
			));
		}

		{
			List<? super CharSequence> to = null;
			List<? super String> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<? super CharSequence>>() {}.getType(),
					new TypeT<List<? super String>>() {}.getType()
			));
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test6() {
		{
			List to = null;
			List<String> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<List>() {}.getType(),
					new TypeT<List<String>>() {}.getType()
			));
		}
		{
			List<String> to = null;
			List<?> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<List<String>>() {}.getType(),
					new TypeT<List<?>>() {}.getType()
			));
		}
		{
			List<String> to = null;
			List from = null;
			to = from;
			assertFalse(isAssignable(
					new TypeT<List<String>>() {}.getType(),
					new TypeT<List>() {}.getType()
			));
		}
		{
			Map<? super String, ? extends List<? extends Number>> to = null;
			HashMap<CharSequence, ArrayList<Integer>> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<Map<? super String, ? extends List<? extends Number>>>() {}.getType(),
					new TypeT<HashMap<CharSequence, ArrayList<Integer>>>() {}.getType()
			));
		}
	}

	@Test
	public void test7() {
		{
			List<? extends Object[]> to = null;
			List<? extends Integer[]> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<Object[]>() {}.getType(),
					new TypeT<Integer[]>() {}.getType()
			));
		}
		{
			List<? extends Integer[]> to = null;
			List<? extends Object[]> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<Integer[]>() {}.getType(),
					new TypeT<Object[]>() {}.getType()
			));
		}
	}

	@Test
	public void test8() {
		{
			TestClass1<Integer> to = null;
			TestClass2<String, Integer> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<TestClass1<Integer>>() {}.getType(),
					new TypeT<TestClass2<String, Integer>>() {}.getType()
			));
		}
		{
			TestClass1<Integer> to = null;
			TestClass2<Integer, Integer> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<TestClass1<Integer>>() {}.getType(),
					new TypeT<TestClass2<Integer, Integer>>() {}.getType()
			));
		}
		{
			TestClass1<? extends Number> to = null;
			TestClass2<String, Integer> from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<TestClass1<? extends Number>>() {}.getType(),
					new TypeT<TestClass2<String, Integer>>() {}.getType()
			));
		}
		{
			TestClass1<Number> to = null;
			TestClass2<String, Integer> from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<TestClass1<Number>>() {}.getType(),
					new TypeT<TestClass2<String, Integer>>() {}.getType()
			));
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test9() {
		assertTrue(isAssignable(
				new TypeT<Collection>() {}.getType(),
				new TypeT<Queue>() {}.getType()
		));
		assertTrue(isAssignable(
				new TypeT<Collection>() {}.getType(),
				new TypeT<Queue<String>>() {}.getType()
		));
		assertFalse(isAssignable(
				new TypeT<Collection<String>>() {}.getType(),
				new TypeT<Queue>() {}.getType()
		));
		assertFalse(isAssignable(
				new TypeT<Collection<? extends String>>() {}.getType(),
				new TypeT<Queue>() {}.getType()
		));
		assertFalse(isAssignable(
				new TypeT<Collection<? super String>>() {}.getType(),
				new TypeT<Queue>() {}.getType()
		));
	}

	@Test
	public void test10() {
		{
			Object[] to = null;
			Integer[] from = null;
			to = from;
			assertTrue(isAssignable(
					new TypeT<Object[]>() {}.getType(),
					new TypeT<Integer[]>() {}.getType()
			));
			assertTrue(isAssignable(
					new GenericArrayTypeImpl(Object.class),
					new GenericArrayTypeImpl(Integer.class)
			));
			assertTrue(isAssignable(
					Object[].class,
					new GenericArrayTypeImpl(Integer.class)
			));
			assertTrue(isAssignable(
					new GenericArrayTypeImpl(Object.class),
					Integer[].class
			));
		}
		{
			Integer[] to = null;
			Object[] from = null;
//			to = from;
			assertFalse(isAssignable(
					new TypeT<Integer[]>() {}.getType(),
					new TypeT<Object[]>() {}.getType()
			));
			assertFalse(isAssignable(
					new GenericArrayTypeImpl(Integer.class),
					new GenericArrayTypeImpl(Object.class)
			));
			assertFalse(isAssignable(
					new GenericArrayTypeImpl(Integer.class),
					Object[].class
			));
			assertFalse(isAssignable(
					Integer[].class,
					new GenericArrayTypeImpl(Object.class)
			));
		}
	}
}
