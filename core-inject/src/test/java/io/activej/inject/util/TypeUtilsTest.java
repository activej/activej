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
		GenericModule2<String, Integer> genericModuleStringInteger = new GenericModule2<>() {};
		GenericModule2<Double, Long> genericModuleDoubleLong = new GenericModule2<>() {};

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

		Set<String> strings = injector.getInstance(new Key<>() {});
		Set<? extends String> stringsExtends = injector.getInstance(new Key<>() {});
		Set<? super String> stringsSuper = injector.getInstance(new Key<>() {});

		assertEquals(Set.of(aString), strings);
		assertSame(strings, stringsExtends);
		assertSame(strings, stringsSuper);

		Set<Integer> integers = injector.getInstance(new Key<>() {});
		Set<? extends Integer> integersExtends = injector.getInstance(new Key<>() {});
		Set<? super Integer> integersSuper = injector.getInstance(new Key<>() {});

		assertEquals(Set.of(anInteger), integers);
		assertSame(integers, integersExtends);
		assertSame(integers, integersSuper);

		Set<Double> doubles = injector.getInstance(new Key<>() {});
		Set<? extends Double> doublesExtends = injector.getInstance(new Key<>() {});
		Set<? super Double> doublesSuper = injector.getInstance(new Key<>() {});

		assertEquals(Set.of(aDouble), doubles);
		assertSame(doubles, doublesExtends);
		assertSame(doubles, doublesSuper);

		Set<Long> longs = injector.getInstance(new Key<>() {});
		Set<? extends Long> longsExtends = injector.getInstance(new Key<>() {});
		Set<? super Long> longsSuper = injector.getInstance(new Key<>() {});

		assertEquals(Set.of(aLong), longs);
		assertSame(longs, longsExtends);
		assertSame(longs, longsSuper);

		{
			List<String> stringsList = injector.getInstance(new Key<>() {});
			assertEquals(List.of(aString), stringsList);

			List<Integer> integersList = injector.getInstance(new Key<>() {});
			assertEquals(List.of(anInteger), integersList);

			List<Double> doublesList = injector.getInstance(new Key<>() {});
			assertEquals(List.of(aDouble), doublesList);

			List<Long> longsList = injector.getInstance(new Key<>() {});
			assertEquals(List.of(aLong), longsList);
		}

		{
			Queue<String> stringsQueue = injector.getInstance(new Key<>() {});
			assertEquals(1, stringsQueue.size());
			assertEquals(aString, stringsQueue.remove());

			Queue<Integer> integersQueue = injector.getInstance(new Key<>() {});
			assertEquals(1, integersQueue.size());
			assertEquals(anInteger, (int) integersQueue.remove());

			Queue<Double> doublesQueue = injector.getInstance(new Key<>() {});
			assertEquals(1, doublesQueue.size());
			assertEquals(aDouble, doublesQueue.remove(), 0.0);

			Queue<Long> longsQueue = injector.getInstance(new Key<>() {});
			assertEquals(1, longsQueue.size());
			assertEquals(aLong, (long) longsQueue.remove());
		}

		{
			Optional<String> stringsOptional = injector.getInstance(new Key<>() {});
			assertEquals(aString, stringsOptional.get());

			Optional<Integer> integersOptional = injector.getInstance(new Key<>() {});
			assertEquals(anInteger, (int) integersOptional.get());

			Optional<Double> doublesOptional = injector.getInstance(new Key<>() {});
			assertEquals(aDouble, doublesOptional.get(), 0.0);

			Optional<Long> longsOptional = injector.getInstance(new Key<>() {});
			assertEquals(aLong, (long) longsOptional.get());
		}
	}

	public static abstract class GenericModule2<A, B extends Number> extends AbstractModule {
		Key<A> aKey = new Key<>() {};
		Key<? extends A> extendsAKey = new Key<>() {};
		Key<? super A> superAKey = new Key<>() {};

		Key<B> bKey = new Key<>() {};
		Key<? extends B> extendsBKey = new Key<>() {};
		Key<? super B> superBKey = new Key<>() {};

		@Override
		protected void configure() {
			bind(Key.ofType(Types.parameterizedType(List.class, aKey.getType()))).to(List::of, aKey);
			bind(Key.ofType(Types.parameterizedType(List.class, bKey.getType()))).to(List::of, bKey);

			bind(Key.ofType(Types.parameterizedType(Queue.class, extendsAKey.getType()))).to(a -> new ArrayDeque<>(Set.of(a)), extendsAKey);
			bind(Key.ofType(Types.parameterizedType(Queue.class, extendsBKey.getType()))).to(b -> new ArrayDeque<>(Set.of(b)), extendsBKey);

			bind(Key.ofType(Types.parameterizedType(Optional.class, superAKey.getType()))).to(Optional::of, superAKey);
			bind(Key.ofType(Types.parameterizedType(Optional.class, superBKey.getType()))).to(Optional::of, superBKey);
		}

		@Provides
		Set<? extends A> genericSetA(A element) {
			return Set.of(element);
		}

		@Provides
		Set<? super B> genericSetB(B element) {
			return Set.of(element);
		}
	}

	public static final class TestClass<A, B, C, D, E, F, G, H, I> {
	}

	interface TestInterface<A, B extends Integer> {
	}

}
