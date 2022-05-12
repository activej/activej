package io.activej.inject;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.util.Trie;
import org.junit.Test;

import java.time.Instant;
import java.util.*;

import static junit.framework.TestCase.*;

/**
 * To represent the main concepts and features of ActiveJ Inject,
 * we've created an example which starts with low-level ActiveJ Inject
 * concepts and gradually covers more specific advanced features.
 * <p>
 * In this example we have a kitchen, where you can
 * automatically create tasty cookies in different ways
 * with our wonderful ActiveJ Inject.
 */
public class DIFollowUpTest {
	//[START REGION_9]
	public static final Scope ORDER_SCOPE = Scope.of(OrderScope.class);
	//[END REGION_9]

	record Kitchen(int places) {
		@Inject
		Kitchen() {
			this(1);
		}
	}

	//[START REGION_8]
	record Sugar(String name, float weight) {
		@Inject
		public Sugar() {
			this("WhiteSugar", 10.f);
		}
	}
	//[END REGION_8]

	record Butter(String name, float weight) {
		@Inject
		public Butter() {
			this("Butter", 10.f);
		}
	}

	record Flour(String name, float weight) {
		@Inject
		public Flour() {
			this(null, 0);
		}
	}

	@Inject
	record Pastry(Sugar sugar, Butter butter, Flour flour) {}

	@Inject
	record Cookie(Pastry pastry) {}

	static class InjectsDefinition {
		@Provides
		static Sugar sugar() {return new Sugar("WhiteSugar", 10.f);}

		@Provides
		static Butter butter() {return new Butter("PerfectButter", 20.0f);}

		@Provides
		static Flour flour() {return new Flour("GoodFlour", 100.0f);}

		@Provides
		static Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
			return new Pastry(sugar, butter, flour);
		}

		@Provides
		static Cookie cookie(Pastry pastry) {
			return new Cookie(pastry);
		}
	}

	@Test
	//[START REGION_1]
	public void manualBindSnippet() {
		Map<Key<?>, Binding<?>> bindings = new LinkedHashMap<>();
		bindings.put(Key.of(Sugar.class), Binding.to(() -> new Sugar("WhiteSugar", 10.0f)));
		bindings.put(Key.of(Butter.class), Binding.to(() -> new Butter("PerfectButter", 20.0f)));
		bindings.put(Key.of(Flour.class), Binding.to(() -> new Flour("GoodFlour", 100.0f)));
		bindings.put(Key.of(Pastry.class), Binding.to(Pastry::new, Sugar.class, Butter.class, Flour.class));
		bindings.put(Key.of(Cookie.class), Binding.to(Cookie::new, Pastry.class));

		Injector injector = Injector.of(Trie.leaf(bindings));
		Cookie instance = injector.getInstance(Cookie.class);

		assertEquals(10.f, instance.pastry().sugar().weight(), 0.0f);
	}
	//[END REGION_1]

	@Test
	//[START REGION_2]
	public void moduleBindSnippet() {
		Module module = ModuleBuilder.create()
				.bind(Sugar.class).to(() -> new Sugar("WhiteSugar", 10.0f))
				.bind(Butter.class).to(() -> new Butter("PerfectButter", 20.0f))
				.bind(Flour.class).to(() -> new Flour("GoodFlour", 100.0f))
				.bind(Pastry.class).to(Pastry::new, Sugar.class, Butter.class, Flour.class)
				.bind(Cookie.class).to(Cookie::new, Pastry.class)
				.build();

		Injector injector = Injector.of(module);
		assertEquals("PerfectButter", injector.getInstance(Cookie.class).pastry().butter().name());
	}
	//[END REGION_2]

	@Test
	//[START REGION_3]
	public void provideAnnotationSnippet() {
		Module cookbook = new AbstractModule() {
			@Provides
			Sugar sugar() {return new Sugar("WhiteSugar", 10.f);}

			@Provides
			Butter butter() {return new Butter("PerfectButter", 20.0f);}

			@Provides
			Flour flour() {return new Flour("GoodFlour", 100.0f);}

			@Provides
			Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			Cookie cookie(Pastry pastry) {
				return new Cookie(pastry);
			}
		};

		Injector injector = Injector.of(cookbook);
		assertEquals("PerfectButter", injector.getInstance(Cookie.class).pastry().butter().name());
	}
	//[END REGION_3]

	@Test
	//[START REGION_4]
	public void scanObjectSnippet() {
		Module cookbook = ModuleBuilder.create()
				.scan(new Object() {
					@Provides
					Sugar sugar() {return new Sugar("WhiteSugar", 10.f);}

					@Provides
					Butter butter() {return new Butter("PerfectButter", 20.0f);}

					@Provides
					Flour flour() {return new Flour("GoodFlour", 100.0f);}

					@Provides
					Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
						return new Pastry(sugar, butter, flour);
					}

					@Provides
					Cookie cookie(Pastry pastry) {
						return new Cookie(pastry);
					}
				})
				.build();

		Injector injector = Injector.of(cookbook);
		assertEquals("PerfectButter", injector.getInstance(Cookie.class).pastry().butter().name());
	}
	//[END REGION_4]

	@Test
	//[START REGION_5]
	public void scanClassSnippet() {
		Module cookbook = ModuleBuilder.create().scan(InjectsDefinition.class).build();

		Injector injector = Injector.of(cookbook);
		assertEquals("PerfectButter", injector.getInstance(Cookie.class).pastry().butter().name());
	}
	//[END REGION_5]

	@Test
	//[START REGION_6]
	public void injectAnnotationSnippet() {
		Module cookbook = ModuleBuilder.create().bind(Cookie.class).build();

		Injector injector = Injector.of(cookbook);
		assertEquals("WhiteSugar", injector.getInstance(Cookie.class).pastry().sugar().name());
	}
	//[END REGION_6]

	@Test
	//[START REGION_7]
	public void namedAnnotationSnippet() {
		Module cookbook = new AbstractModule() {
			@Provides
			@Named("zerosugar")
			Sugar sugar1() {return new Sugar("SugarFree", 0.f);}

			@Provides
			@Named("normal")
			Sugar sugar2() {return new Sugar("WhiteSugar", 10.f);}

			@Provides
			Butter butter() {return new Butter("PerfectButter", 20.f);}

			@Provides
			Flour flour() {return new Flour("GoodFlour", 100.f);}

			@Provides
			@Named("normal")
			Pastry pastry1(@Named("normal") Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			@Named("zerosugar")
			Pastry pastry2(@Named("zerosugar") Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			@Named("normal")
			Cookie cookie1(@Named("normal") Pastry pastry) {
				return new Cookie(pastry);
			}

			@Provides
			@Named("zerosugar")
			Cookie cookie2(@Named("zerosugar") Pastry pastry) {return new Cookie(pastry);}
		};

		Injector injector = Injector.of(cookbook);

		float normalWeight = injector.getInstance(Key.of(Cookie.class, "normal"))
				.pastry().sugar().weight();
		float zerosugarWeight = injector.getInstance(Key.of(Cookie.class, "zerosugar"))
				.pastry().sugar().weight();

		assertEquals(10.f, normalWeight, 0.0f);
		assertEquals(0.f, zerosugarWeight, 0.0f);
	}
	//[END REGION_7]

	@Test
	//[START REGION_10]
	public void moduleBuilderWithQualifiedBindsSnippet() {
		Module cookbook = ModuleBuilder.create()
				.bind(Key.of(Sugar.class, "zerosugar")).to(() -> new Sugar("SugarFree", 0.f))
				.bind(Key.of(Sugar.class, "normal")).to(() -> new Sugar("WhiteSugar", 10.f))
				.bind(Key.of(Pastry.class, "zerosugar")).to(Pastry::new, Key.of(Sugar.class).qualified("zerosugar"), Key.of(Butter.class), Key.of(Flour.class))
				.bind(Key.of(Pastry.class, "normal")).to(Pastry::new, Key.of(Sugar.class).qualified("normal"), Key.of(Butter.class), Key.of(Flour.class))
				.bind(Key.of(Cookie.class, "zerosugar")).to(Cookie::new, Key.of(Pastry.class).qualified("zerosugar"))
				.bind(Key.of(Cookie.class, "normal")).to(Cookie::new, Key.of(Pastry.class).qualified("normal"))
				.build();

		Injector injector = Injector.of(cookbook);

		float normalWeight = injector.getInstance(Key.of(Cookie.class, "normal"))
				.pastry().sugar().weight();
		float zerosugarWeight = injector.getInstance(Key.of(Cookie.class, "zerosugar"))
				.pastry().sugar().weight();

		assertEquals(10.f, normalWeight, 0.0f);
		assertEquals(0.f, zerosugarWeight, 0.0f);
	}
	//[END REGION_10]

	@Test
	public void orderAnnotationSnippet() {
		//[START REGION_11]
		Module cookbook = ModuleBuilder.create()
				.bind(Kitchen.class).to(Kitchen::new)
				.bind(Sugar.class).to(Sugar::new).in(OrderScope.class)
				.bind(Butter.class).to(Butter::new).in(OrderScope.class)
				.bind(Flour.class).to(Flour::new).in(OrderScope.class)
				.bind(Pastry.class).to(Pastry::new, Sugar.class, Butter.class, Flour.class).in(OrderScope.class)
				.bind(Cookie.class).to(Cookie::new, Pastry.class).in(OrderScope.class)
				.build();
		//[END REGION_11]

		//[START REGION_12]
		Injector injector = Injector.of(cookbook);
		Kitchen kitchen = injector.getInstance(Kitchen.class);
		Set<Cookie> cookies = Collections.newSetFromMap(new IdentityHashMap<>());
		for (int i = 0; i < 10; ++i) {
			Injector subinjector = injector.enterScope(ORDER_SCOPE);

			assertSame(subinjector.getInstance(Kitchen.class), kitchen);
			if (i > 0) assertFalse(cookies.contains(subinjector.getInstance(Cookie.class)));

			cookies.add(subinjector.getInstance(Cookie.class));
		}
		assertEquals(10, cookies.size());
		//[END REGION_12]
	}

	@Test
	//[START REGION_13]
	public void transformBindingSnippet() {
		Module cookbook = ModuleBuilder.create()
				.bind(Sugar.class).to(Sugar::new)
				.bind(Butter.class).to(Butter::new)
				.bind(Flour.class).to(() -> new Flour("GoodFlour", 100.0f))
				.bind(Pastry.class).to(Pastry::new, Sugar.class, Butter.class, Flour.class)
				.bind(Cookie.class).to(Cookie::new, Pastry.class)
				.transform(Object.class, (bindings, scope, key, binding) ->
						binding.onInstance(x -> System.out.println(Instant.now() + " -> " + key)))
				.build();

		Injector injector = Injector.of(cookbook);
		assertEquals("GoodFlour", injector.getInstance(Cookie.class).pastry().flour().name());
	}
	//[END REGION_13]
}
