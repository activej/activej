package io.activej.inject;

import io.activej.common.ApplicationSettings;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
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
public class ActiveJDiScopesBenchmark {
	private static final boolean SPECIALIZE = ApplicationSettings.getBoolean(ActiveJDiScopesBenchmark.class, "specialize", false);

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
		public Flour() {}

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

	static class Cookie7 {
		private final Pastry pastry;

		@Inject
		Cookie7(Pastry pastry) {
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

	public static final Scope ORDER_SCOPE = Scope.of(OrderScope.class);

	@Setup
	public void setup() {
		cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				bind(CookieBucket.class).to(CookieBucket::new, Cookie1.class, Cookie2.class,
						Cookie3.class, Cookie4.class, Cookie5.class, Cookie6.class).in(OrderScope.class);
			}

			@Provides
			Kitchen kitchen() {return new Kitchen();}

			@OrderScope
			Sugar sugar() {return new Sugar("WhiteSugar", 10.f);}

			@OrderScope
			Butter butter() {return new Butter("PerfectButter", 20.0f);}

			@OrderScope
			Flour flour() {return new Flour("GoodFlour", 100.0f);}

			@OrderScope
			Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@OrderScope
			Cookie1 cookie1(Pastry pastry) {
				return new Cookie1(pastry);
			}

			@OrderScope
			Cookie2 cookie2(Pastry pastry) {
				return new Cookie2(pastry);
			}

			@OrderScope
			Cookie3 cookie3(Pastry pastry) {
				return new Cookie3(pastry);
			}

			@OrderScope
			Cookie4 cookie4(Pastry pastry) {
				return new Cookie4(pastry);
			}

			@OrderScope
			Cookie5 cookie5(Pastry pastry) {
				return new Cookie5(pastry);
			}

			@OrderScope
			Cookie6 cookie6(Pastry pastry) {
				return new Cookie6(pastry);
			}

			@OrderScope
			Cookie7 cookie7(Pastry pastry) {
				return new Cookie7(pastry);
			}
		};

		if (SPECIALIZE) {
			Injector.useSpecializer();
		}
		injector = Injector.of(cookbook);
	}

	@Param({"1", "10"})
	int arg;

	@Benchmark
	@OutputTimeUnit(TimeUnit.NANOSECONDS)
	public void testMethod(Blackhole blackhole) {
		Kitchen kitchen = injector.getInstance(Kitchen.class);
		for (int i = 0; i < arg; ++i) {
			Injector subInjector = injector.enterScope(ORDER_SCOPE);
			cb = subInjector.getInstance(CookieBucket.class);
			blackhole.consume(cb);
		}
		blackhole.consume(kitchen);
	}

	public static void main(String[] args) throws RunnerException {

		Options opt = new OptionsBuilder()
				.include(ActiveJDiScopesBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(3)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(10)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}
}

//  master (24.07)
//	Benchmark                       (arg)  Mode  Cnt     Score     Error  Units
//	ActiveJDiScopesBenchmark.testMethod      0  avgt   20    50.301 ±   0.340  ns/op
//	ActiveJDiScopesBenchmark.testMethod      1  avgt   20   934.922 ±  16.528  ns/op
//	ActiveJDiScopesBenchmark.testMethod     10  avgt   20  8658.261 ± 138.339  ns/op

// 29.07, threadsafe = true.
//	Benchmark                       (arg)  Mode  Cnt     Score    Error  Units
//	ActiveJDiScopesBenchmark.testMethod      0  avgt   20    41.203 ±  0.330  ns/op
//	ActiveJDiScopesBenchmark.testMethod      1  avgt   20   404.545 ±  5.134  ns/op
//	ActiveJDiScopesBenchmark.testMethod     10  avgt   20  3482.703 ± 72.545  ns/o



