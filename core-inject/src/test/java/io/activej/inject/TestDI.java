package io.activej.inject;

import io.activej.inject.annotation.*;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.DIException;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.impl.Preprocessor;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.inject.util.Constructors.Constructor0;
import io.activej.inject.util.Trie;
import io.activej.inject.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.activej.inject.binding.BindingGenerators.combinedGenerator;
import static io.activej.inject.binding.BindingTransformers.combinedTransformer;
import static io.activej.inject.binding.BindingType.TRANSIENT;
import static io.activej.inject.binding.Multibinders.combinedMultibinder;
import static io.activej.inject.module.Modules.combine;
import static io.activej.inject.module.Modules.override;
import static io.activej.inject.util.Utils.printGraphVizGraph;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class TestDI {

	@Before
	public void setUp() {
		StringInterface.counter = 0;
	}

	@Test
	public void basic() {
		Module module = ModuleBuilder.create()
				.bind(Integer.class).toInstance(42)
				.bind(String.class).to(i -> "str: " + i, Integer.class)
				.build();

		Injector injector = Injector.of(module);

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void singletons() {
		AtomicInteger ref = new AtomicInteger(41);
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(Integer.class).to(ref::incrementAndGet)
				.bind(String.class).to(i -> "str: " + i, Integer.class)
				.build());

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void provider() {
		AtomicInteger ref = new AtomicInteger(41);
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(Integer.class).to(ref::incrementAndGet)
				.bind(String.class).to(i -> "str: " + i.get(), new Key<InstanceProvider<Integer>>() {})
				.bindInstanceProvider(String.class)
				.build());

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));

		InstanceProvider<String> provider = injector.getInstance(new Key<InstanceProvider<String>>() {});
		assertEquals("str: 42", provider.get());
		assertEquals("str: 42", provider.get());
		assertEquals("str: 42", provider.get());

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals(42, injector.getInstance(Integer.class).intValue());
	}

	@Test
	public void eagers() {
		AtomicInteger mut = new AtomicInteger();

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(String.class).to(() -> "str_" + mut.incrementAndGet()).asEager()
				.bind(Object.class).to(() -> "whatever")
				.build());

		injector.createEagerInstances();

		assertEquals("str_1", injector.peekInstance(String.class));
		assertNull(injector.peekInstance(Object.class));

		injector.peekInstance(Float.class);
	}

	@Test
	public void crossmodule() {
		Injector injector = Injector.of(
				ModuleBuilder.create()
						.bind(Integer.class).toInstance(42)
						.build(),
				ModuleBuilder.create()
						.bind(String.class).to(i -> "str: " + i, Integer.class)
						.build());

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void overrides() {
		Injector injector = Injector.of(override(
				ModuleBuilder.create()
						.bind(Integer.class).toInstance(17)
						.bind(String.class).to(i -> "str: " + i, Integer.class)
						.build(),
				ModuleBuilder.create()
						.bind(Integer.class).toInstance(42)
						.build()));

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void duplicates() {
		Module module = ModuleBuilder.create()
				.bind(Integer.class).toInstance(42)
				.bind(String.class).to(i -> "str1: " + i, Integer.class)
				.bind(String.class).to(i -> "str2: " + i, Integer.class)
				.build();
		try {
			Injector.of(module);
			fail("should've failed");
		} catch (DIException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().startsWith("Duplicate bindings for key String"));
		}
	}

	@Test
	public void simpleCycle() {
		Module module = ModuleBuilder.create()
				.bind(Integer.class).to($ -> 42, String.class)
				.bind(String.class).to(i -> "str: " + i, Integer.class)
				.build();

		try {
			Injector.of(module);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void advancedCycles() {

		// branch that leads to the cycle(s) (should not be in exception output)
		Module branch = ModuleBuilder.create()
				.bind(Short.class).to(Integer::shortValue, Integer.class)
				.bind(Byte.class).to(Short::byteValue, Short.class)
				.build();

		Module cyclic1 = ModuleBuilder.create()
				.bind(Integer.class).to($ -> 42, Object.class)
				.bind(Object.class).to($ -> new Object(), String.class)
				.bind(String.class).to(i -> "str: " + i, Float.class)
				.bind(Float.class).to(i -> (float) i, Integer.class)
				.build();

		try {
			Injector.of(branch, cyclic1);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
		}

		Module cyclic2 = ModuleBuilder.create()
				.bind(Double.class).to($ -> 42.0, Character.class)
				.bind(Character.class).to($ -> 'k', Boolean.class)
				.bind(Boolean.class).to($ -> Boolean.TRUE, Double.class)
				.build();

		try {
			Injector.of(branch, cyclic1, cyclic2);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void dsl() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.scan(new Object() {
					@Provides
					String string(Integer integer) {
						return "str: " + integer;
					}

					@Provides
					Integer integer() {
						return 42;
					}
				})
				.build());

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void namedDsl() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.scan(new Object() {
					@Provides
					String string(@Named("test") Integer integer) {
						return "str: " + integer;
					}

					@Provides
					@Named("test")
					Integer integer1() {
						return 42;
					}

					@Provides
					@Named("test2")
					Integer integer2() {
						return 43;
					}

					@Provides
					Integer integer() {
						return -1;
					}
				})
				.build());

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void injectDsl() {
		class ClassWithCustomDeps {
			@Inject
			@Named("test")
			String string;

			@Inject
			Integer raw;
		}

		Injector injector = Injector.of(ModuleBuilder.create()
				.bindInstanceInjector(ClassWithCustomDeps.class)
				.scan(new Object() {
					@Provides
					ClassWithCustomDeps classWithCustomDeps() {
						return new ClassWithCustomDeps();
					}

					@Provides
					@Named("test")
					String testString(Integer integer) {
						return "str: " + integer;
					}

					@Provides
					Integer integer() {
						return 42;
					}
				})
				.build());

		ClassWithCustomDeps instance = injector.getInstance(ClassWithCustomDeps.class);
		assertNull(instance.string);
		assertNull(instance.raw);
		InstanceInjector<ClassWithCustomDeps> instanceInjector = injector.getInstance(new Key<InstanceInjector<ClassWithCustomDeps>>() {});
		instanceInjector.injectInto(instance);
		assertEquals("str: 42", instance.string);
		assertEquals(42, instance.raw.intValue());
	}

	@Test
	public void inheritedInjects() {
		class ClassWithCustomDeps {

			@Inject
			String string;
		}

		@Inject
		class Inherited extends ClassWithCustomDeps {
		}

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(TestDI.class).toInstance(TestDI.this) // inherited class has implicit dependency on enclosing class
				.bind(Inherited.class)
				.bind(String.class).to(i -> "str: " + i, Integer.class)
				.bind(Integer.class).toInstance(42)
				.build());

		Inherited instance = injector.getInstance(Inherited.class);
		assertEquals("str: 42", instance.string);
	}

	@Inject
	static class RecursiveA {

		@Inject
		RecursiveB dependency;
	}

	@Inject
	static class RecursiveB {

		@Inject
		RecursiveA dependency;
	}

	@Test
	public void cyclicInjects() {
		Module module = ModuleBuilder.create().bind(RecursiveA.class).build();
		try {
			Injector.of(module);
			fail("should've detected the cycle and fail");
		} catch (DIException e) {
			e.printStackTrace();
		}
	}

	static class RecursiveX {
		@SuppressWarnings({"FieldCanBeLocal", "unused"})
		private final RecursiveY y;

		@Inject
		RecursiveX(RecursiveY y) {
			this.y = y;
		}
	}

	static class RecursiveY {
		private final InstanceProvider<RecursiveX> xProvider;

		@Inject
		RecursiveY(InstanceProvider<RecursiveX> provider) {
			xProvider = provider;
		}
	}

	@Test
	public void cyclicInjects2() {
		Injector injector = Injector.of(ModuleBuilder.create().bind(RecursiveX.class).build());

		RecursiveX x = injector.getInstance(RecursiveX.class);
		RecursiveY y = injector.getInstance(RecursiveY.class);
		assertSame(x, y.xProvider.get());
	}

	@Test
	public void optionalInjects() {

		@Inject
		class ClassWithCustomDeps {

			@Inject
			OptionalDependency<String> stringOpt;

			@Inject
			Integer integer;
		}

		Module module = ModuleBuilder.create()
				.bind(TestDI.class).toInstance(TestDI.this)
				.bind(ClassWithCustomDeps.class)
				.bind(Integer.class).toInstance(42)
				.build();
		Injector injector = Injector.of(module);

		ClassWithCustomDeps instance = injector.getInstance(ClassWithCustomDeps.class);
		assertFalse(instance.stringOpt.isPresent());
		assertEquals(42, instance.integer.intValue());

		Injector injector2 = Injector.of(module, ModuleBuilder.create().bind(String.class).to(i -> "str: " + i, Integer.class).build());

		ClassWithCustomDeps instance2 = injector2.getInstance(ClassWithCustomDeps.class);
		assertTrue(instance2.stringOpt.isPresent());
		assertEquals("str: 42", instance2.stringOpt.get());
		assertEquals(42, instance2.integer.intValue());

		Module module2 = ModuleBuilder.create()
				.bind(TestDI.class).toInstance(TestDI.this)
				.bind(ClassWithCustomDeps.class)
				.bind(String.class).toInstance("str")
				.build();
		try {
			Injector.of(module2);
			fail("should've failed, but didn't");
		} catch (DIException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().startsWith("Unsatisfied dependencies detected:\n\tkey Integer required"));
		}
	}

	static class MyServiceImpl {
		final String string;
		int value = 0;

		private MyServiceImpl(String string) {
			this.string = string;
		}

		@Inject
		public void setValue(int value) {
			this.value = value;
		}

		@Inject
		static MyServiceImpl create(String string) {
			System.out.println("factory method called once");
			return new MyServiceImpl(string);
		}
	}

	@Test
	public void injectFactoryMethod() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(MyServiceImpl.class)
				.bind(String.class).to(() -> "hello")
				.bind(int.class).toInstance(43)
				.build());

		MyServiceImpl service = injector.getInstance(MyServiceImpl.class);

		assertEquals("hello", service.string);
		assertEquals(43, service.value);
	}

	@SuppressWarnings("unused")
	@Inject
	static class Container<Z, T, U> {

		@Inject
		T something;

		@Inject
		U somethingElse;
	}

	@Test
	public void simpleGeneric() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(String.class).toInstance("hello")
				.bind(Integer.class).toInstance(42)
				.bind(new Key<Container<Float, String, Integer>>() {})
				.build());

		Container<Float, String, Integer> instance = injector.getInstance(new Key<Container<Float, String, Integer>>() {});
		assertEquals("hello", instance.something);
		assertEquals(42, instance.somethingElse.intValue());
	}

	@Test
	public void templatedProvider() {

		class Container<T> {
			private final T object;

			public Container(T object) {
				this.object = object;
			}
		}

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(new Key<Container<String>>() {})
				.bind(new Key<Container<Integer>>() {})
				.scan(new Object() {
					@Provides
					<T> Container<T> container(T t) {
						return new Container<>(t);
					}

					@Provides
					String string() {
						return "hello";
					}

					@Provides
					Integer integer() {
						return 42;
					}
				})
				.build());

		assertEquals("hello", injector.getInstance(new Key<Container<String>>() {}).object);
		assertEquals(42, injector.getInstance(new Key<Container<Integer>>() {}).object.intValue());
	}

	@Test
	public void optionalProvidesParam() {
		Module module = ModuleBuilder.create()
				.scan(new Object() {
					@Provides
					String string(Integer integer, OptionalDependency<Float> floatOpt) {
						return "str: " + integer + ", " + floatOpt.orElse(null);
					}

					@Provides
					Integer integer() {
						return 42;
					}
				})
				.build();

		Injector injector = Injector.of(module);
		assertEquals("str: 42, null", injector.getInstance(String.class));

		Injector injector2 = Injector.of(combine(module, ModuleBuilder.create().bind(Float.class).toInstance(3.14f).build()));
		assertEquals("str: 42, 3.14", injector2.getInstance(String.class));
	}

	@Test
	public void providesIntoSet() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.scan(new Object() {
					@Provides
					Integer integer() {
						return 42;
					}

					@ProvidesIntoSet
					String string1(Integer integer) {
						return "str1: " + integer;
					}

					@ProvidesIntoSet
					String string2(Integer integer) {
						return "str2: " + integer;
					}

					@ProvidesIntoSet
					String string3(Integer integer) {
						return "str3: " + integer;
					}

					@ProvidesIntoSet
					List<String> stringsB1(Integer integer) {
						return singletonList("str1: " + integer);
					}

					@ProvidesIntoSet
					List<String> stringsB2(Integer integer) {
						return singletonList("str2: " + integer);
					}

				})
				.build());

		Set<String> instance = injector.getInstance(new Key<Set<String>>() {});

		Set<String> expected = Stream.of("str1: 42", "str2: 42", "str3: 42").collect(toSet());

		assertEquals(expected, instance);

		Key<Set<List<String>>> key = new Key<Set<List<String>>>() {};
		Set<List<String>> instance2 = injector.getInstance(key);

		Set<List<String>> expected2 = Stream.of(singletonList("str1: 42"), singletonList("str2: 42")).collect(toSet());

		assertEquals(expected2, instance2);
	}

	@Test
	public void inheritedProviders() {
		class ObjectWithProviders {
			@Provides
			Integer integer() {
				return 123;
			}
		}

		class ObjectWithProviders2 extends ObjectWithProviders {
			@Provides
			String string(Integer integer) {
				return integer.toString();
			}
		}

		Injector injector = Injector.of(ModuleBuilder.create().scan(new ObjectWithProviders2()).build());
		String string = injector.getInstance(String.class);

		assertEquals("123", string);
	}

	@Test
	public void abstractModuleGenerics() {

		@Inject
		class AndAContainerToo<T> {

			@Inject
			@Named("namedGeneric")
			T object;
		}

		abstract class Module1<D> extends AbstractModule {
			@Provides
			String string(D object) {
				return "str: " + object.toString();
			}
		}

		abstract class Module2<C> extends Module1<C> {
			@Override
			protected void configure() {
				bind(TestDI.class).toInstance(TestDI.this);
				bind(new Key<AndAContainerToo<C>>() {});
			}
/*
			@Provides
			<T extends Number> List<T> generator(T instance, List<C> object) {
				return object.isEmpty() ? emptyList() : singletonList(instance);
			}
*/

			@Provides
			@Named("second")
			String string(List<C> object) {
				return "str: " + object.toString();
			}
		}

		Injector injector = Injector.of(new Module2<Integer>() {
			@Override
			protected void configure() {
				super.configure();
				bind(Integer.class).toInstance(42);
				bind(Integer.class, "namedGeneric").toInstance(-42);
				bind(new Key<List<Integer>>() {}).toInstance(asList(1, 2, 3));
//				bind(new Key<List<Long>>() {});
//				bind(Long.class).toInstance(-42L);
			}
		});

		Utils.printGraphVizGraph(injector.getBindingsTrie());

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: [1, 2, 3]", injector.getInstance(Key.of(String.class, "second")));
		assertEquals(-42, injector.getInstance(new Key<AndAContainerToo<Integer>>() {}).object.intValue());
//		assertEquals(null, injector.getInstance(new Key<List<Long>>() {}));
	}

	@Test
	public void injectConstructor() {

		class Injectable {
			final String string;
			final Integer integer;

			@Inject
			Injectable(String string, OptionalDependency<Integer> integerOpt) {
				this.string = string;
				this.integer = integerOpt.orElse(null);
			}
		}

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(TestDI.class).toInstance(TestDI.this)
				.bind(Injectable.class)
				.bind(String.class).toInstance("hello")
				.build());

		Injectable instance = injector.getInstance(Injectable.class);
		assertEquals("hello", instance.string);
		assertNull(instance.integer);
	}

	@Test
	public void transitiveImplicitBinding() {
		@Inject
		class Container {
			@Inject
			InstanceProvider<InstanceProvider<String>> provider;
		}

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(TestDI.class).toInstance(TestDI.this)
				.bind(Container.class)
				.bind(String.class).toInstance("hello")
				.build());

		Container instance = injector.getInstance(Container.class);

		assertEquals("hello", instance.provider.get().get());
	}

	@Test
	public void mapMultibinding() {

		Key<Map<String, Integer>> key = new Key<Map<String, Integer>>() {};

		Injector injector = Injector.of(ModuleBuilder.create()
				.multibindToMap(String.class, Integer.class)
				.scan(new Object() {
					@Provides
					Integer integer() {
						return 42;
					}

					@Provides
					Map<String, Integer> firstOne() {
						return singletonMap("first", 1);
					}

					@Provides
					Map<String, Integer> second(Integer integer) {
						return singletonMap("second", integer);
					}

					@Provides
					Map<String, Integer> thirdTwo() {
						return singletonMap("third", 2);
					}
				})
				.build());

		Map<String, Integer> map = injector.getInstance(key);

		Map<String, Integer> expected = new HashMap<>();
		expected.put("first", 1);
		expected.put("second", 42);
		expected.put("third", 2);

		assertEquals(expected, map);
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@ScopeAnnotation
	@interface Scope1 {
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@ScopeAnnotation
	@interface Scope2 {
	}

	@Test
	public void ignoringScopes() {
		Module module = ModuleBuilder.create()
				.scan(new Object() {
					@Provides
					@Scope1
					Double first() {
						return 27d;
					}

					@Provides
					@Scope2
					Float second(Double first, Integer top) {
						// static check runs on injector creation, so it won't fail
						// (unsatisfied Double from other scope)
						return 34f;
					}

					@Provides
					Integer top() {
						return 42;
					}

					@Provides
					@Scopes({Scope1.class, Scope2.class})
					String deeper(Integer top, Double first) {
						return "deeper";
					}
				})
				.build();

		printGraphVizGraph(reduceModuleBindings(module));

		Trie<Scope, Map<Key<?>, Binding<?>>> flattened = reduceModuleBindings(Modules.ignoreScopes(module));

		printGraphVizGraph(flattened);

		assertEquals(0, flattened.getChildren().size());
		assertEquals(Stream.of(Double.class, Float.class, Integer.class, String.class)
				.map(Key::of)
				.collect(toSet()), flattened.get().keySet());
	}

	private static @NotNull Trie<Scope, Map<Key<?>, Binding<?>>> reduceModuleBindings(Module module) {
		return Preprocessor.reduce(
				module.getBindings(),
				combinedMultibinder(module.getMultibinders()),
				combinedTransformer(module.getBindingTransformers()),
				combinedGenerator(module.getBindingGenerators()));
	}

	@Test
	public void restrictedContainer() {

		class Container<T> {
			final T peer;

			public Container(T object) {
				this.peer = object;
			}
		}

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(Integer.class).toInstance(42)
				.bind(Float.class).toInstance(34f)
				.bind(Byte.class).toInstance((byte) -1)
				.bind(String.class).toInstance("hello")
				.bind(new Key<Container<Float>>() {})
				.bind(new Key<Container<Byte>>() {})
				.bind(new Key<Container<Integer>>() {})
				.bind(new Key<Container<String>>() {})
				.scan(new Object() {
					@Provides
					<T extends Number> Container<T> provide(T number) {
						System.out.println("called number provider");
						return new Container<>(number);
					}

					@Provides
					<T extends CharSequence> Container<T> provide2(T str) {
						System.out.println("called string provider");
						return new Container<>(str);
					}
				})
				.build());

		assertEquals(42, injector.getInstance(new Key<Container<Integer>>() {}).peer.intValue());
		assertEquals("hello", injector.getInstance(new Key<Container<String>>() {}).peer);
	}

	@Test
	public void annotatedTemplate() {

		class Container<T> {
			final T peer;

			public Container(T object) {
				this.peer = object;
			}
		}

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(String.class).toInstance("hello")
				.bind(new Key<Container<String>>("hello") {})
				.scan(new Object() {
					@Provides
					@Named("hello")
					<T> Container<T> provide(T number) {
						return new Container<>(number);
					}
				})
				.build());

		System.out.println(injector.getInstance(new Key<Container<String>>("hello") {}).peer);
	}

	@Test
	public void methodLocalClass() {

		String captured = "captured";

		@Inject
		class MethodLocal {

			MethodLocal() {
			}

			@SuppressWarnings("unused")
			String captured() {
				return captured;
			}
		}
		Module module = ModuleBuilder.create().bind(MethodLocal.class).build();
		try {
			Injector.of(module);
			fail("Should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().contains("inject annotation on local class that closes over outside variables and/or has no default constructor"));
		}
	}

	@Test
	public void recursiveTemplate() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(new Key<Comparator<String>>() {})
				.bind(new Key<Comparator<Integer>>() {})
				.scan(new Object() {
					@Provides
					<T extends Comparable<? super T>> Comparator<T> naturalComparator() {
						return Comparator.naturalOrder();
					}
				})
				.build());

		assertEquals(Comparator.naturalOrder(), injector.getInstance(new Key<Comparator<String>>() {}));
		assertEquals(Comparator.naturalOrder(), injector.getInstance(new Key<Comparator<Integer>>() {}));
	}

	@Test
	public void uninterruptibeBindRequests() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(String.class)
				.bind(String.class)
				.bind(String.class)
				.scan(new Object() {
					@Provides
					String string() {
						return "hello";
					}
				})
				.build());

		assertEquals("hello", injector.getInstance(String.class));
	}

	@Test
	public void scopedBindFail() {
		Module module1 = ModuleBuilder.create().bind(String.class).in(Scope1.class).build();
		Module module2 = ModuleBuilder.create().bind(String.class).toInstance("root string").build();
		try {
			Injector.of(module1, module2);
			fail("Should've failed");
		} catch (DIException e) {
			assertTrue(e.getMessage().startsWith("Refused to generate an explicitly requested binding for key String"));
		}
	}

	@Test
	public void scopedBindWin() {
		Injector injector = Injector.of(
				ModuleBuilder.create().bind(String.class).in(Scope1.class).build(),
				ModuleBuilder.create().bind(String.class).in(Scope1.class).toInstance("scoped string").build());

		Injector subInjector = injector.enterScope(Scope.of(Scope1.class));

		assertEquals("scoped string", subInjector.getInstance(String.class));
	}

	static class MyModule extends AbstractModule {}

	static class InheritedModule extends MyModule {}

	@ShortTypeName("RenamedModule")
	static class OtherModule extends MyModule {}

	@SuppressWarnings("unused")
	static class GenericModule<A, B> extends AbstractModule {}

	@Test
	public void abstractModuleToString() {
		Module module = new AbstractModule() {};
		Module module2 = new MyModule();
		Module module3 = new InheritedModule();
		Module module4 = new GenericModule<String, Integer>() {};
		Module module5 = new OtherModule();

		System.out.println(module2);

		assertTrue(module.toString().startsWith("AbstractModule(at io.activej.inject.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module2.toString().startsWith("MyModule(at io.activej.inject.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module3.toString().startsWith("InheritedModule(at io.activej.inject.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module4.toString().startsWith("GenericModule<String, Integer>(at io.activej.inject.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module5.toString().startsWith("RenamedModule(at io.activej.inject.TestDI.abstractModuleToString(TestDI.java:"));
	}

	@Test
	public void changeDisplayName() {

		@ShortTypeName("GreatPojoName")
		class Pojo {}
		class PlainPojo {}

		@SuppressWarnings("unused")
		@ShortTypeName("GreatGenericPojoName")
		class GenericPojo<A, B> {}
		@SuppressWarnings("unused")
		class PlainGenericPojo<A, B> {}

		assertEquals("PlainPojo", Key.of(PlainPojo.class).getDisplayString());
		assertEquals("GreatPojoName", Key.of(Pojo.class).getDisplayString());

		assertEquals("PlainGenericPojo<Integer, List<String>>", new Key<PlainGenericPojo<Integer, List<String>>>() {}.getDisplayString());
		assertEquals("GreatGenericPojoName<Integer, List<String>>", new Key<GenericPojo<Integer, List<String>>>() {}.getDisplayString());
	}

	public interface TestInterface<T> {
		T getObj();
	}

	public static class StringInterface implements TestInterface<String> {
		static int counter = 0;

		private final String obj;

		public StringInterface(String obj) {
			this.obj = obj;
			counter++;
		}

		@Override
		public String getObj() {
			return obj;
		}
	}

	@Test
	public void bindIntoSetBug() {
		Injector injector = Injector.of(
				new AbstractModule() {
					@Override
					protected void configure() {
						bindIntoSet(new Key<TestInterface<?>>() {}, Key.of(StringInterface.class));
					}

					@Provides
					StringInterface testBindIntoSet() {
						return new StringInterface("string");
					}
				});
		Set<TestInterface<?>> interfaces = injector.getInstance(new Key<Set<TestInterface<?>>>() {});

		injector.getInstance(StringInterface.class);

		assertEquals(1, interfaces.size());
		assertEquals(1, StringInterface.counter); // bug: 1 != 2
	}

	public interface PostConstruct {
		void init();
	}

	public static class PostConstructModule extends AbstractModule {
		@Override
		protected void configure() {
			transform(PostConstruct.class, (bindings, scope, key, binding) ->
					binding.mapInstance(obj -> {
						obj.init();
						return obj;
					}));
		}
	}

	public static class PostConstructed implements PostConstruct {
		private final String s;
		private boolean initialized;

		public PostConstructed(String s) {
			this.s = s;
		}

		@Override
		public void init() {
			initialized = true;
		}

		@Override
		public String toString() {
			return s;
		}
	}

	@Test
	public void transientMap() {
		AtomicInteger mut = new AtomicInteger();
		Injector injector = Injector.of(ModuleBuilder.create()
						.bind(PostConstructed.class).to(() -> new PostConstructed("str_" + mut.incrementAndGet())).asTransient()
						.build(),
				new PostConstructModule());

		PostConstructed instance1 = injector.getInstance(PostConstructed.class);
		assertEquals("str_1", instance1.s);
		assertTrue(instance1.initialized);
		PostConstructed instance2 = injector.getInstance(PostConstructed.class);
		assertEquals("str_2", instance2.s);
		assertTrue(instance2.initialized);
		PostConstructed instance3 = injector.getInstance(PostConstructed.class);
		assertEquals("str_3", instance3.s);
		assertTrue(instance3.initialized);
	}

	@Test
	public void transientBinding() {
		AtomicInteger mut = new AtomicInteger();
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(Integer.class).to(mut::incrementAndGet).asTransient()
				.bind(String.class, "fixed").to(i -> "str_" + i, Integer.class)
				.bind(String.class).to(i -> "str_" + i, Integer.class).asTransient()
				.build());

		assertEquals(5, Stream.generate(() -> injector.getInstance(Integer.class)).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(Key.of(String.class, "fixed"))).limit(5).collect(toSet()).size());
		assertEquals(5, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
	}

	@Test
	public void transientDsl() {
		AtomicInteger counter = new AtomicInteger();
		AtomicInteger mut = new AtomicInteger();

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(String.class).to(() -> {
					counter.incrementAndGet();
					return "str_";
				})
				.bind(Integer.class).to(mut::incrementAndGet).asTransient()
				.scan(new Object() {
					@Provides
					@Named("t")
					@Transient
					String string(String s, Integer i) {
						return s + i;
					}

					@Provides
					@Named("nt")
					String string2(String s, Integer i) {
						return s + i;
					}
				})
				.build());

		assertEquals(5, Stream.generate(() -> injector.getInstance(Key.of(String.class, "t"))).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(Key.of(String.class, "nt"))).limit(5).collect(toSet()).size());
		assertEquals(7, injector.getInstance(Integer.class).intValue());
		assertEquals(1, counter.get());
	}

	@Test
	public void transientGenerators() {
		Key<Set<String>> stringSetKey = new Key<Set<String>>() {};
		{
			AtomicInteger mut = new AtomicInteger();
			Injector injector = Injector.of(ModuleBuilder.create()
					.bind(stringSetKey)
					.bind(String.class)
					.generate(String.class, (bindings, scope, key) ->
							Binding.to(() -> "str_" + mut.incrementAndGet()))
					.scan(new Object() {
						@Provides
						<T> Set<T> set(T t) {
							return singleton(t);
						}
					})
					.build());
			assertEquals(1, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
			assertEquals(1, Stream.generate(() -> injector.getInstance(stringSetKey)).limit(5).collect(toSet()).size());
		}
		{
			AtomicInteger mut = new AtomicInteger();
			Injector injector = Injector.of(ModuleBuilder.create()
					.bind(stringSetKey)
					.bind(String.class)
					.generate(String.class, (bindings, scope, key) ->
							Binding.to(() -> "str_" + mut.incrementAndGet()).as(TRANSIENT))
					.scan(new Object() {
						@Provides
						@Transient
						<T> Set<T> set(T t) {
							return singleton(t);
						}
					})
					.build());
			assertEquals(5, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
			assertEquals(5, Stream.generate(() -> injector.getInstance(stringSetKey)).limit(5).collect(toSet()).size());
		}
		{
			AtomicInteger mut = new AtomicInteger();
			Injector injector = Injector.of(ModuleBuilder.create()
					.bind(stringSetKey)
					.bind(String.class)
					.generate(String.class, (bindings, scope, key) ->
							Binding.to(() -> "str_" + mut.incrementAndGet()).as(TRANSIENT))
					.scan(new Object() {
						@Provides
						<T> Set<T> set(T t) {
							return singleton(t);
						}
					})
					.build());
			assertEquals(5, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
			assertEquals(1, Stream.generate(() -> injector.getInstance(stringSetKey)).limit(5).collect(toSet()).size());
		}
	}

	@Test
	public void transientPlainBind() {
		AtomicInteger mut = new AtomicInteger();
		{
			Injector injector = Injector.of(ModuleBuilder.create()
					.bind(String.class)
					.generate(String.class, (bindings, scope, key) ->
							Binding.to(() -> "str_" + mut.incrementAndGet()))
					.build());
			assertEquals(1, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
		}
		{
			Injector injector = Injector.of(ModuleBuilder.create()
					.bind(String.class)
					.generate(String.class, (bindings, scope, key) ->
							Binding.to(() -> "str_" + mut.incrementAndGet()).as(TRANSIENT))
					.build());

			assertEquals(5, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
		}
	}

	@Test
	public void partiallyTransientMultibind() {
		AtomicInteger mut = new AtomicInteger();
		Constructor0<Set<String>> constructor = () -> singleton("str_" + mut.incrementAndGet());

		Key<Set<String>> setKey = new Key<Set<String>>() {};
		Key<Set<String>> setKeyNt = setKey.qualified("nt");

		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(setKey).to(constructor).asTransient()
				.bind(setKey).toInstance(singleton("other one"))

				.bind(setKeyNt).to(constructor)
				.bind(setKeyNt).toInstance(singleton("other one"))

				.bind(new Key<Set<String>>() {})

				.multibindToSet(String.class)
				.multibindToSet(Key.of(String.class, "nt"))
				.build());

		assertEquals(5, Stream.generate(() -> injector.getInstance(setKey)).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(setKeyNt)).limit(5).collect(toSet()).size());
	}

	@Test
	public void plainBindPeekInstance() {
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(PostConstructed.class).to(() -> new PostConstructed("hello world"))
				.bind(PostConstruct.class).to(PostConstructed.class)
				.build());

		PostConstruct instance = injector.getInstance(PostConstruct.class);
		assertEquals(instance, injector.peekInstance(PostConstruct.class));
	}

	@Test
	public void restrictAndRemap() {
		Module module = Modules.restrict(
				ModuleBuilder.create()
						.bind(String.class, "exported").to(i -> "str: " + i, Key.of(Integer.class, "internal1"))
						.bind(Integer.class, "internal1").to(Key.of(Integer.class, "internal2"))
						.bind(Integer.class, "internal2").to(v -> v, Key.of(Integer.class, "internal3"))
						.bind(Integer.class, "internal3").to(Key.of(Integer.class, "imported"))
						.build(),
				Key.of(String.class, "exported"));

		assertEquals(singleton(Key.of(String.class, "exported")), module.getExports().get());
		assertEquals(singleton(Key.of(Integer.class, "imported")), module.getImports().get());

		{
			Injector injector = Injector.of(
					module,
					ModuleBuilder.create().bind(Integer.class, "imported").toInstance(42).build());

			String instance = injector.getInstance(Key.of(String.class, "exported"));
			assertEquals("str: 42", instance);
		}
		{
			Map<Key<?>, Key<?>> remapping = new HashMap<>();
			remapping.put(Key.of(Integer.class), Key.of(Integer.class, "imported"));
			remapping.put(Key.of(String.class), Key.of(String.class, "exported"));

			Injector injector = Injector.of(
					Modules.remap(module, remapping),
					ModuleBuilder.create().bind(Integer.class).toInstance(42).build());

			String instance = injector.getInstance(Key.of(String.class));
			assertEquals("str: 42", instance);
		}

	}

	@Test
	public void implicitKey() {
		Key<Key<String>> keyOfKeyOfString = new Key<Key<String>>() {};
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(keyOfKeyOfString)
				.build());
		Key<String> instance = injector.getInstance(keyOfKeyOfString);

		assertEquals(String.class, instance.getType());
	}

	@Test
	public void injectClass() {
		Key<TestClass> key = new Key<TestClass>() {};
		String expected = "Hello, World";
		Injector injector = Injector.of(ModuleBuilder.create()
				.bind(String.class).to(() -> expected)
				.bind(key)
				.build());
		TestClass instance = injector.getInstance(key);

		assertEquals(expected, instance.getText());
	}

	public static final class TestClass {
		private final String text;

		@Inject
		public TestClass(String text) {
			this.text = text;
		}

		public String getText() {
			return text;
		}
	}

}
