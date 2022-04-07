package io.activej.inject.util;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.ModuleBuilder;
import io.activej.types.TypeT;
import io.activej.types.Types;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.*;

import static io.activej.inject.util.TypeUtils.simplifyType;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TypeUtilsTest {
	@Test
	public void testSimplifyType() {
		{
			Type type = Integer.class;
			assertEquals(type, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<Integer>>() {}.getType();
			assertEquals(type, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<Set<Set<Integer>>>>() {}.getType();
			assertEquals(type, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<? extends Integer>>() {}.getType();
			Type expected = new TypeT<Set<Integer>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<? extends Set<? extends Set<? extends Integer>>>>() {}.getType();
			Type expected = new TypeT<Set<Set<Set<Integer>>>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<Set<? extends Set<Integer>>>>() {}.getType();
			Type expected = new TypeT<Set<Set<Set<Integer>>>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<? super Integer>>() {}.getType();
			Type expected = new TypeT<Set<Integer>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<? super Set<? super Set<? super Integer>>>>() {}.getType();
			Type expected = new TypeT<Set<Set<Set<Integer>>>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<Set<? super Set<Integer>>>>() {}.getType();
			Type expected = new TypeT<Set<Set<Set<Integer>>>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<? extends Set<? super Set<? extends Integer>>>>() {}.getType();
			Type expected = new TypeT<Set<Set<Set<Integer>>>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<? extends Integer>[]>() {}.getType();
			Type expected = new TypeT<Set<Integer>[]>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<Set<? super Integer>[]>() {}.getType();
			Type expected = new TypeT<Set<Integer>[]>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<TestInterface<? extends Integer, ? extends Integer>>() {}.getType();
			Type expected = new TypeT<TestInterface<Integer, Integer>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<TestInterface<Integer, Integer>>() {}.getType();
			Type expected = new TypeT<TestInterface<Integer, Integer>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<TestClass<?, ?, ?, ?, ?, ?, ?, ?, ?>>() {}.getType();
			Type expected = TestClass.class;
			assertEquals(expected, simplifyType(type));
		}

		{
			Type type = new TypeT<TestClass<?, ?, ?, Object, ?, ?, ?, ?, ?>>() {}.getType();
			Type expected = TestClass.class;
			assertEquals(expected, simplifyType(type));
		}

		{
			//noinspection TypeParameterExplicitlyExtendsObject
			Type type = new TypeT<TestClass<
					Integer,
					? extends Integer,
					? super Integer,
					Object,
					? extends Object,
					? super Object,
					?,
					Set<? extends TestInterface<Integer, ? extends Integer>>,
					Set<? super TestInterface<Integer, ? super Integer>>
					>>() {}.getType();
			Type expected = new TypeT<TestClass<
					Integer,
					Integer,
					Integer,
					Object,
					Object,
					Object,
					Object,
					Set<TestInterface<Integer, Integer>>,
					Set<TestInterface<Integer, Integer>>
					>>() {}.getType();
			assertEquals(expected, simplifyType(type));
		}
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	@Test
	public void genericModule() {
		GenericModule2<String, Integer> genericModuleStringInteger = new GenericModule2<String, Integer>() {};
		GenericModule2<Double, Long> genericModuleDoubleLong = new GenericModule2<Double, Long>() {};

		String aString = "test";
		int anInteger = 1;
		double aDouble = 3.14;
		long aLong = Long.MAX_VALUE;
		Injector injector = Injector.of(genericModuleDoubleLong, genericModuleStringInteger,
				ModuleBuilder.create()
						.bind(String.class).to(() -> aString)
						.bind(Integer.class).to(() -> anInteger)
						.bind(Double.class).to(() -> aDouble)
						.bind(Long.class).to(() -> aLong)
						.build());

		Set<String> strings = injector.getInstance(new Key<Set<String>>() {});
		Set<? extends String> stringsExtends = injector.getInstance(new Key<Set<? extends String>>() {});
		Set<? super String> stringsSuper = injector.getInstance(new Key<Set<? super String>>() {});

		assertEquals(singleton(aString), strings);
		assertSame(strings, stringsExtends);
		assertSame(strings, stringsSuper);

		Set<Integer> integers = injector.getInstance(new Key<Set<Integer>>() {});
		Set<? extends Integer> integersExtends = injector.getInstance(new Key<Set<? extends Integer>>() {});
		Set<? super Integer> integersSuper = injector.getInstance(new Key<Set<? super Integer>>() {});

		assertEquals(singleton(anInteger), integers);
		assertSame(integers, integersExtends);
		assertSame(integers, integersSuper);

		Set<Double> doubles = injector.getInstance(new Key<Set<Double>>() {});
		Set<? extends Double> doublesExtends = injector.getInstance(new Key<Set<? extends Double>>() {});
		Set<? super Double> doublesSuper = injector.getInstance(new Key<Set<? super Double>>() {});

		assertEquals(singleton(aDouble), doubles);
		assertSame(doubles, doublesExtends);
		assertSame(doubles, doublesSuper);

		Set<Long> longs = injector.getInstance(new Key<Set<Long>>() {});
		Set<? extends Long> longsExtends = injector.getInstance(new Key<Set<? extends Long>>() {});
		Set<? super Long> longsSuper = injector.getInstance(new Key<Set<? super Long>>() {});

		assertEquals(singleton(aLong), longs);
		assertSame(longs, longsExtends);
		assertSame(longs, longsSuper);

		{
			List<String> stringsList = injector.getInstance(new Key<List<String>>() {});
			assertEquals(singletonList(aString), stringsList);

			List<Integer> integersList = injector.getInstance(new Key<List<Integer>>() {});
			assertEquals(singletonList(anInteger), integersList);

			List<Double> doublesList = injector.getInstance(new Key<List<Double>>() {});
			assertEquals(singletonList(aDouble), doublesList);

			List<Long> longsList = injector.getInstance(new Key<List<Long>>() {});
			assertEquals(singletonList(aLong), longsList);
		}

		{
			Queue<String> stringsQueue = injector.getInstance(new Key<Queue<String>>() {});
			assertEquals(1, stringsQueue.size());
			assertEquals(aString, stringsQueue.remove());

			Queue<Integer> integersQueue = injector.getInstance(new Key<Queue<Integer>>() {});
			assertEquals(1, integersQueue.size());
			assertEquals(anInteger, (int) integersQueue.remove());

			Queue<Double> doublesQueue = injector.getInstance(new Key<Queue<Double>>() {});
			assertEquals(1, doublesQueue.size());
			assertEquals(aDouble, doublesQueue.remove(), 0.0);

			Queue<Long> longsQueue = injector.getInstance(new Key<Queue<Long>>() {});
			assertEquals(1, longsQueue.size());
			assertEquals(aLong, (long) longsQueue.remove());
		}

		{
			Optional<String> stringsOptional = injector.getInstance(new Key<Optional<String>>() {});
			assertEquals(aString, stringsOptional.get());

			Optional<Integer> integersOptional = injector.getInstance(new Key<Optional<Integer>>() {});
			assertEquals(anInteger, (int) integersOptional.get());

			Optional<Double> doublesOptional = injector.getInstance(new Key<Optional<Double>>() {});
			assertEquals(aDouble, doublesOptional.get(), 0.0);

			Optional<Long> longsOptional = injector.getInstance(new Key<Optional<Long>>() {});
			assertEquals(aLong, (long) longsOptional.get());
		}
	}

	public static abstract class GenericModule2<A, B extends Number> extends AbstractModule {
		Key<A> aKey = new Key<A>() {};
		Key<? extends A> extendsAKey = new Key<A>() {};
		Key<? super A> superAKey = new Key<A>() {};

		Key<B> bKey = new Key<B>() {};
		Key<? extends B> extendsBKey = new Key<B>() {};
		Key<? super B> superBKey = new Key<B>() {};

		@Override
		protected void configure() {
			bind(Key.ofType(Types.parameterizedType(List.class, aKey.getType()))).to(Collections::singletonList, aKey);
			bind(Key.ofType(Types.parameterizedType(List.class, bKey.getType()))).to(Collections::singletonList, bKey);

			bind(Key.ofType(Types.parameterizedType(Queue.class, extendsAKey.getType()))).to(a -> new ArrayDeque<>(singleton(a)), extendsAKey);
			bind(Key.ofType(Types.parameterizedType(Queue.class, extendsBKey.getType()))).to(b -> new ArrayDeque<>(singleton(b)), extendsBKey);

			bind(Key.ofType(Types.parameterizedType(Optional.class, superAKey.getType()))).to(Optional::of, superAKey);
			bind(Key.ofType(Types.parameterizedType(Optional.class, superBKey.getType()))).to(Optional::of, superBKey);
		}

		@Provides
		Set<? extends A> genericSetA(A element) {
			return singleton(element);
		}

		@Provides
		Set<? super B> genericSetB(B element) {
			return singleton(element);
		}
	}

	public static final class TestClass<A, B, C, D, E, F, G, H, I> {
	}

	interface TestInterface<A, B extends Integer> {
	}

}
