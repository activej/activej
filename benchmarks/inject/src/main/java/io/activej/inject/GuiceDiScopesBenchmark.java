package io.activej.inject;

import com.google.inject.Injector;
import com.google.inject.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * @since 3.0.0
 */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@SuppressWarnings("FieldCanBeLocal")
public class GuiceDiScopesBenchmark {

	static class Kitchen {
		private final int places;

		@Inject
		Kitchen() {
			this.places = 1;
		}

		public int getPlaces() {
			return places;
		}
	}

	static class Sugar {
		private final String name;
		private final float weight;

		@Inject
		public Sugar() {
			this.name = "WhiteSugar";
			this.weight = 10.f;
		}
		//[END REGION_8]

		public Sugar(String name, float weight) {
			this.name = name;
			this.weight = weight;
		}

		public String getName() {
			return name;
		}

		public float getWeight() {
			return weight;
		}
	}

	static class Butter {
		private final float weight;
		private final String name;

		@Inject
		public Butter() {
			this.weight = 10.f;
			this.name = "Butter";
		}

		public Butter(String name, float weight) {
			this.weight = weight;
			this.name = name;
		}

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}

	static class Flour {
		private float weight;
		private String name;

		@Inject
		public Flour() { }

		public Flour(String name, float weight) {
			this.weight = weight;
			this.name = name;
		}

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}

	static class Pastry {
		private final Sugar sugar;
		private final Butter butter;
		private final Flour flour;

		@Inject
		Pastry(Sugar sugar, Butter butter, Flour flour) {
			this.sugar = sugar;
			this.butter = butter;
			this.flour = flour;
		}

		public Flour getFlour() {
			return flour;
		}

		public Sugar getSugar() {
			return sugar;
		}

		public Butter getButter() {
			return butter;
		}
	}

	static class Cookie1 {
		private final Pastry pastry;

		@Inject
		Cookie1(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie2 {
		private final Pastry pastry;

		@Inject
		Cookie2(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie3 {
		private final Pastry pastry;

		@Inject
		Cookie3(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie4 {
		private final Pastry pastry;

		@Inject
		Cookie4(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie5 {
		private final Pastry pastry;

		@Inject
		Cookie5(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie6 {
		private final Pastry pastry;

		@Inject
		Cookie6(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class CookieBucket {
		private final Cookie1 c1;
		private final Cookie2 c2;
		private final Cookie3 c3;
		private final Cookie4 c4;
		private final Cookie5 c5;
		private final Cookie6 c6;

		public Cookie4 getC4() {
			return c4;
		}

		@Inject
		public CookieBucket(Cookie1 c1, Cookie2 c2, Cookie3 c3, Cookie4 c4, Cookie5 c5, Cookie6 c6) {
			this.c1 = c1;
			this.c2 = c2;
			this.c3 = c3;
			this.c4 = c4;
			this.c5 = c5;
			this.c6 = c6;
		}
	}

	AbstractModule cookbook;
	Injector injector;
	CookieBucket cb;

	@Setup
	public void setup() {
		cookbook = new AbstractModule() {
			@Override
			public void configure() {
				SimpleScope orderScope = new SimpleScope();

				// tell Guice about the scope
				bindScope(GuiceOrder.class, orderScope);
			}

			@Provides
			@Singleton
			Kitchen kitchen() { return new Kitchen(); }

			@Provides
			@GuiceOrder
			Sugar sugar() { return new Sugar("WhiteSugar", 10.f); }

			@Provides
			@GuiceOrder
			Butter butter() { return new Butter("PerfectButter", 20.0f); }

			@Provides
			@GuiceOrder
			Flour flour() { return new Flour("GoodFlour", 100.0f); }

			@Provides
			@GuiceOrder
			Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			@GuiceOrder
			Cookie1 cookie1(Pastry pastry) {
				return new Cookie1(pastry);
			}

			@Provides
			@GuiceOrder
			Cookie2 cookie2(Pastry pastry) {
				return new Cookie2(pastry);
			}

			@Provides
			@GuiceOrder
			Cookie3 cookie3(Pastry pastry) {
				return new Cookie3(pastry);
			}

			@Provides
			@GuiceOrder
			Cookie4 cookie4(Pastry pastry) {
				return new Cookie4(pastry);
			}

			@Provides
			@GuiceOrder
			Cookie5 cookie5(Pastry pastry) {
				return new Cookie5(pastry);
			}

			@Provides
			@GuiceOrder
			Cookie6 cookie6(Pastry pastry) {
				return new Cookie6(pastry);
			}

			@Provides
			@GuiceOrder
			CookieBucket tort(Cookie1 c1, Cookie2 c2, Cookie3 c3, Cookie4 c4, Cookie5 c5, Cookie6 c6) {
				return new CookieBucket(c1, c2, c3, c4, c5, c6);
			}

		};
		injector = Guice.createInjector(cookbook);
	}

	@Param({"1", "10"})
	public int arg;

	@Benchmark
	@OutputTimeUnit(TimeUnit.NANOSECONDS)
	public void measure(Blackhole blackhole) {
		Kitchen kitchen = injector.getInstance(Kitchen.class);
		for (int i = 0; i < arg; ++i) {
			SimpleScope.enter();
			cb = injector.getInstance(CookieBucket.class);
			blackhole.consume(cb);
			SimpleScope.exit();
		}
		blackhole.consume(kitchen);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(GuiceDiScopesBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(3)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(10)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.shouldDoGC(false)
				.build();

		new Runner(opt).run();
	}
}
