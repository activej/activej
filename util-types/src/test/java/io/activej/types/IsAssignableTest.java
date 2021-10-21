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

	@Test
	public void typeVariables() throws NoSuchMethodException {
		{
			assertTrue(isAssignable(new TypeT<List<String>>() {}.getType(), Interface.class.getMethod("listOfT").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<Integer>>() {}.getType(), Interface.class.getMethod("listOfT").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<Object>>() {}.getType(), Interface.class.getMethod("listOfT").getGenericReturnType()));

			assertFalse(isAssignable(new TypeT<List<String>>() {}.getType(), Interface.class.getMethod("listOfTExtendsNumber").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<Integer>>() {}.getType(), Interface.class.getMethod("listOfTExtendsNumber").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<Object>>() {}.getType(), Interface.class.getMethod("listOfTExtendsNumber").getGenericReturnType()));

			assertTrue(isAssignable(new TypeT<List<String>>() {}.getType(), Interface.class.getMethod("listOfTExtendsObject").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<Integer>>() {}.getType(), Interface.class.getMethod("listOfTExtendsObject").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<Object>>() {}.getType(), Interface.class.getMethod("listOfTExtendsObject").getGenericReturnType()));
		}

		{
			assertTrue(isAssignable(Interface.class.getMethod("listOfT").getGenericReturnType(), new TypeT<List<String>>() {}.getType()));
			assertTrue(isAssignable(Interface.class.getMethod("listOfT").getGenericReturnType(), new TypeT<List<Integer>>() {}.getType()));
			assertTrue(isAssignable(Interface.class.getMethod("listOfT").getGenericReturnType(), new TypeT<List<Object>>() {}.getType()));

			assertFalse(isAssignable(Interface.class.getMethod("listOfTExtendsNumber").getGenericReturnType(), new TypeT<List<String>>() {}.getType()));
			assertTrue(isAssignable(Interface.class.getMethod("listOfTExtendsNumber").getGenericReturnType(), new TypeT<List<Integer>>() {}.getType()));
			assertFalse(isAssignable(Interface.class.getMethod("listOfTExtendsNumber").getGenericReturnType(), new TypeT<List<Object>>() {}.getType()));

			assertTrue(isAssignable(Interface.class.getMethod("listOfTExtendsObject").getGenericReturnType(), new TypeT<List<String>>() {}.getType()));
			assertTrue(isAssignable(Interface.class.getMethod("listOfTExtendsObject").getGenericReturnType(), new TypeT<List<Integer>>() {}.getType()));
			assertTrue(isAssignable(Interface.class.getMethod("listOfTExtendsObject").getGenericReturnType(), new TypeT<List<Object>>() {}.getType()));
		}
	}

	@Test
	public void typeVariablesMultipleGenerics() throws NoSuchMethodException {
		{
			assertTrue(isAssignable(new TypeT<Map<String, String>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKV").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<Map<Integer, Integer>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKV").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<Map<Object, Object>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKV").getGenericReturnType()));

			assertFalse(isAssignable(new TypeT<Map<String, String>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsNumberV").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<Map<Integer, Integer>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsNumberV").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<Map<Object, Object>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsNumberV").getGenericReturnType()));

			assertFalse(isAssignable(new TypeT<Map<String, String>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKVExtendsNumber").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<Map<Integer, Integer>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKVExtendsNumber").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<Map<Object, Object>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKVExtendsNumber").getGenericReturnType()));

			assertFalse(isAssignable(new TypeT<Map<String, String>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsNumberVExtendsNumber").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<Map<Integer, Integer>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsNumberVExtendsNumber").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<Map<Object, Object>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsNumberVExtendsNumber").getGenericReturnType()));

			assertTrue(isAssignable(new TypeT<Map<String, String>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsObjectVExtendsObject").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<Map<Integer, Integer>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsObjectVExtendsObject").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<Map<Object, Object>>() {}.getType(), MultipleGenerics.class.getMethod("mapOfKExtendsObjectVExtendsObject").getGenericReturnType()));
		}

		{
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKV").getGenericReturnType(), new TypeT<Map<String, String>>() {}.getType()));
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKV").getGenericReturnType(), new TypeT<Map<Integer, Integer>>() {}.getType()));
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKV").getGenericReturnType(), new TypeT<Map<Object, Object>>() {}.getType()));

			assertFalse(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsNumberV").getGenericReturnType(), new TypeT<Map<String, String>>() {}.getType()));
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsNumberV").getGenericReturnType(), new TypeT<Map<Integer, Integer>>() {}.getType()));
			assertFalse(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsNumberV").getGenericReturnType(), new TypeT<Map<Object, Object>>() {}.getType()));

			assertFalse(isAssignable(MultipleGenerics.class.getMethod("mapOfKVExtendsNumber").getGenericReturnType(), new TypeT<Map<String, String>>() {}.getType()));
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKVExtendsNumber").getGenericReturnType(), new TypeT<Map<Integer, Integer>>() {}.getType()));
			assertFalse(isAssignable(MultipleGenerics.class.getMethod("mapOfKVExtendsNumber").getGenericReturnType(), new TypeT<Map<Object, Object>>() {}.getType()));

			assertFalse(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsNumberVExtendsNumber").getGenericReturnType(), new TypeT<Map<String, String>>() {}.getType()));
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsNumberVExtendsNumber").getGenericReturnType(), new TypeT<Map<Integer, Integer>>() {}.getType()));
			assertFalse(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsNumberVExtendsNumber").getGenericReturnType(), new TypeT<Map<Object, Object>>() {}.getType()));

			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsObjectVExtendsObject").getGenericReturnType(), new TypeT<Map<String, String>>() {}.getType()));
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsObjectVExtendsObject").getGenericReturnType(), new TypeT<Map<Integer, Integer>>() {}.getType()));
			assertTrue(isAssignable(MultipleGenerics.class.getMethod("mapOfKExtendsObjectVExtendsObject").getGenericReturnType(), new TypeT<Map<Object, Object>>() {}.getType()));
		}
	}

	@Test
	public void typeVariablesMultipleBounds() throws NoSuchMethodException {
		{
			assertFalse(isAssignable(new TypeT<List<String>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<Object>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<A>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<B>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClass>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassA>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<TestClassAPlusB>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<ExtendsTestClassAPlusB>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassB>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<TestClassBPlusA>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<ExtendsTestClassBPlusA>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<TestClassAAndB>>() {}.getType(), MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType()));

			assertFalse(isAssignable(new TypeT<List<String>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<Object>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<A>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<B>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClass>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassA>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<TestClassAPlusB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<ExtendsTestClassAPlusB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassBPlusA>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<ExtendsTestClassBPlusA>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassAAndB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType()));

			assertFalse(isAssignable(new TypeT<List<String>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<Object>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<A>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<B>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClass>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassA>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassAPlusB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<ExtendsTestClassAPlusB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<TestClassBPlusA>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertTrue(isAssignable(new TypeT<List<ExtendsTestClassBPlusA>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
			assertFalse(isAssignable(new TypeT<List<TestClassAAndB>>() {}.getType(), MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType()));
		}

		{
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<String>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<Object>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<A>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<B>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<TestClass>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<TestClassA>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<TestClassAPlusB>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<ExtendsTestClassAPlusB>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<TestClassB>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<TestClassBPlusA>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<ExtendsTestClassBPlusA>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfAandB").getGenericReturnType(), new TypeT<List<TestClassAAndB>>() {}.getType()));

			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<String>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<Object>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<A>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<B>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<TestClass>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<TestClassA>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<TestClassAPlusB>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<ExtendsTestClassAPlusB>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<TestClassB>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<TestClassBPlusA>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<ExtendsTestClassBPlusA>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassAAndB").getGenericReturnType(), new TypeT<List<TestClassAAndB>>() {}.getType()));

			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<String>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<Object>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<A>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<B>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<TestClass>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<TestClassA>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<TestClassAPlusB>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<ExtendsTestClassAPlusB>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<TestClassB>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<TestClassBPlusA>>() {}.getType()));
			assertTrue(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<ExtendsTestClassBPlusA>>() {}.getType()));
			assertFalse(isAssignable(MultipleBounds.class.getMethod("listOfClassBAndA").getGenericReturnType(), new TypeT<List<TestClassAAndB>>() {}.getType()));
		}
	}

	interface Interface {
		<T> List<T> listOfT();

		<T extends Number> List<T> listOfTExtendsNumber();

		<T extends Object> List<T> listOfTExtendsObject();

		default void test() {
			List<String> tAsString = listOfT();
			List<Integer> tAsInteger = listOfT();
			List<Object> tAsObject = listOfT();

//			List<String> tExtendsNumberAsString = listOfTExtendsNumber();
			List<Integer> tExtendsNumberAsInteger = listOfTExtendsNumber();
//			List<Object> tExtendsNumberAsObject = listOfTExtendsNumber();

			List<String> tExtendsObjectAsString = listOfTExtendsObject();
			List<Integer> tExtendsObjectAsInteger = listOfTExtendsObject();
			List<Object> tExtendsObjectAsObject = listOfTExtendsObject();
		}
	}

	interface MultipleGenerics {
		<K, V> Map<K, V> mapOfKV();

		<K extends Number, V> Map<K, V> mapOfKExtendsNumberV();

		<K, V extends Number> Map<K, V> mapOfKVExtendsNumber();

		<K extends Number, V extends Number> Map<K, V> mapOfKExtendsNumberVExtendsNumber();

		<K extends Object, V extends Object> Map<K, V> mapOfKExtendsObjectVExtendsObject();

		default void test() {
			Map<String, String> kAsStringVAsString = mapOfKV();
			Map<Integer, Integer> kAsIntegerVAsInteger = mapOfKV();
			Map<Object, Object> kAsObjectVAsObject = mapOfKV();

//			Map<String, String> kExtendsNumberAsStringVAsString = mapOfKExtendsNumberV();
			Map<Integer, Integer> kExtendsNumberAsIntegerVAsInteger = mapOfKExtendsNumberV();
//			Map<Object, Object> kExtendsNumberAsObjectVAsObject = mapOfKExtendsNumberV();

//			Map<String, String> kAsStringVExtendsNumberAsString = mapOfKVExtendsNumber();
			Map<Integer, Integer> kAsIntegerVExtendsNumberAsInteger = mapOfKVExtendsNumber();
//			Map<Object, Object> kAsObjectVExtendsNumberAsObject = mapOfKVExtendsNumber();

//			Map<String, String> kExtendsNumberAsStringVExtendsNumberAsString = mapOfKExtendsNumberVExtendsNumber();
			Map<Integer, Integer> kExtendsNumberAsIntegerVExtendsNumberAsInteger = mapOfKExtendsNumberVExtendsNumber();
//			Map<Object, Object> kExtendsNumberAsObjectVExtendsNumberAsObject = mapOfKExtendsNumberVExtendsNumber();

			Map<String, String> kExtendsObjectAsStringVExtendsObjectAsString = mapOfKExtendsObjectVExtendsObject();
			Map<Integer, Integer> kExtendsObjectAsIntegerVExtendsObjectAsInteger = mapOfKExtendsObjectVExtendsObject();
			Map<Object, Object> kExtendsObjectAsObjectVExtendsObjectAsObject = mapOfKExtendsObjectVExtendsObject();
		}
	}

	interface MultipleBounds {
		<T extends A & B> List<T> listOfAandB();

		<T extends TestClassA & B> List<T> listOfClassAAndB();

		<T extends TestClassB & A> List<T> listOfClassBAndA();

		default void test() {
//			List<String> tAsStringList1 = listOfAandB();
//			List<Object> tAsObjectList1 = listOfAandB();
//			List<A> tAsAList1 = listOfAandB();
//			List<B> tAsBList1 = listOfAandB();
//			List<TestClass> tAsTestClassList1 = listOfAandB();
//			List<TestClassA> tAsTestClassAList1 = listOfAandB();
			List<TestClassAPlusB> tAsTestClassAPlusBList1 = listOfAandB();
			List<ExtendsTestClassAPlusB> tAsExtendsTestClassAPlusBList1 = listOfAandB();
//			List<TestClassB> tAsTestClassBList1 = listOfAandB();
			List<TestClassBPlusA> tAsTestClassBPlusAList1 = listOfAandB();
			List<ExtendsTestClassBPlusA> tAsExtendsTestClassBPlusAList1 = listOfAandB();
			List<TestClassAAndB> tAsTestClassAAndBList1 = listOfAandB();

//			List<String> tAsStringList2 = listOfClassAAndB();
//			List<Object> tAsObjectList2 = listOfClassAAndB();
//			List<A> tAsAList2 = listOfClassAAndB();
//			List<B> tAsBList2 = listOfClassAAndB();
//			List<TestClass> tAsTestClassList2 = listOfClassAAndB();
//			List<TestClassA> tAsTestClassAList2 = listOfClassAAndB();
			List<TestClassAPlusB> tAsTestClassAPlusBList2 = listOfClassAAndB();
			List<ExtendsTestClassAPlusB> tAsExtendsTestClassAPlusBList2 = listOfClassAAndB();
//			List<TestClassB> tAsTestClassBList2 = listOfClassAAndB();
//			List<TestClassBPlusA> tAsTestClassBPlusAList2 = listOfClassAAndB();
//			List<ExtendsTestClassBPlusA> tAsExtendsTestClassBPlusAList2 = listOfClassAAndB();
//			List<TestClassAAndB> tAsTestClassAAndBList2 = listOfClassAAndB();

//			List<String> tAsStringList3 = listOfClassBAndA();
//			List<Object> tAsObjectList3 = listOfClassBAndA();
//			List<A> tAsAList3 = listOfClassBAndA();
//			List<B> tAsBList3 = listOfClassBAndA();
//			List<TestClass> tAsTestClassList3 = listOfClassBAndA();
//			List<TestClassA> tAsTestClassAList3 = listOfClassBAndA();
//			List<TestClassAPlusB> tAsTestClassAPlusBList3 = listOfClassBAndA();
//			List<ExtendsTestClassAPlusB> tAsExtendsTestClassAPlusBList3 = listOfClassBAndA();
//			List<TestClassB> tAsTestClassBList3 = listOfClassBAndA();
			List<TestClassBPlusA> tAsTestClassBPlusAList3 = listOfClassBAndA();
			List<ExtendsTestClassBPlusA> tAsExtendsTestClassBPlusAList3 = listOfClassBAndA();
//			List<TestClassAAndB> tAsTestClassAAndBList3 = listOfClassBAndA();
		}
	}

	interface A {}

	interface B {}

	class TestClass {}

	class TestClassA extends TestClass implements A {}

	class TestClassAPlusB extends TestClassA implements B {}

	class ExtendsTestClassAPlusB extends TestClassAPlusB {}

	class TestClassB extends TestClass implements B {}

	class TestClassBPlusA extends TestClassB implements A {}

	class ExtendsTestClassBPlusA extends TestClassBPlusA {}

	class TestClassAAndB extends TestClass implements A, B {}
}
