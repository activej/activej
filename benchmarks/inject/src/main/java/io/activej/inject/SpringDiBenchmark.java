package io.activej.inject;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * @since 3.0.0
 */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@Configuration
@SuppressWarnings("FieldCanBeLocal")
public class SpringDiBenchmark {
	static class Kitchen {
		private final int places;

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

		@Autowired
		public Sugar() {
			this.name = "WhiteSugar";
			this.weight = 10.f;
		}
		//[END REGION_8]

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

		@Autowired
		public Butter() {
			this.weight = 10.f;
			this.name = "Butter";
		}

		public float getWeight() {
			return weight;
		}

		public String getName() {
			return name;
		}
	}

	static class Flour {
		private final float weight;
		private final String name;

		@Autowired
		public Flour() {
			this.name = "GoodFlour";
			this.weight = 100.0f;
		}

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

		@Autowired
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

		@Autowired
		Cookie1(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie2 {
		private final Pastry pastry;

		@Autowired
		Cookie2(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie3 {
		private final Pastry pastry;

		@Autowired
		Cookie3(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie4 {
		private final Pastry pastry;

		@Autowired
		Cookie4(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie5 {
		private final Pastry pastry;

		@Autowired
		Cookie5(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	static class Cookie6 {
		private final Pastry pastry;

		@Autowired
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

		@Autowired
		CookieBucket(Cookie1 c1, Cookie2 c2, Cookie3 c3, Cookie4 c4, Cookie5 c5, Cookie6 c6) {
			this.c1 = c1;
			this.c2 = c2;
			this.c3 = c3;
			this.c4 = c4;
			this.c5 = c5;
			this.c6 = c6;
		}
	}

	@Bean
	Kitchen kitchen() {return new Kitchen();}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Sugar sugar() {return new Sugar();}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Butter butter() {return new Butter();}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Flour flour() {return new Flour();}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
		return new Pastry(sugar, butter, flour);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie1 cookie1(Pastry pastry) {
		return new Cookie1(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie2 cookie2(Pastry pastry) {
		return new Cookie2(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie3 cookie3(Pastry pastry) {
		return new Cookie3(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie4 cookie4(Pastry pastry) {
		return new Cookie4(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie5 cookie5(Pastry pastry) {
		return new Cookie5(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	Cookie6 cookie6(Pastry pastry) {
		return new Cookie6(pastry);
	}

	@Bean
	@org.springframework.context.annotation.Scope("prototype")
	CookieBucket tort(Cookie1 c1, Cookie2 c2, Cookie3 c3, Cookie4 c4, Cookie5 c5, Cookie6 c6) {
		return new CookieBucket(c1, c2, c3, c4, c5, c6);
	}

	ConfigurableApplicationContext context;
	CookieBucket cb;

	@Param({"0", "1", "10"})
	int arg;

	@Setup
	public void setup() {
		context = new AnnotationConfigApplicationContext(SpringDiBenchmark.class);
	}

	@Benchmark
	public void measure(Blackhole blackhole) {
		Kitchen kitchen = context.getBean(Kitchen.class);
		for (int i = 0; i < arg; ++i) {
			cb = context.getBean(CookieBucket.class);
			blackhole.consume(cb);
		}
		blackhole.consume(kitchen);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(SpringDiBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(3)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(10)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.MICROSECONDS)
				.build();

		new Runner(opt).run();
	}
}

// 29.07
//	Benchmark                  (arg)  Mode  Cnt     Score    Error  Units
//	SpringDiBenchmark.measure      0  avgt   20    18.583 ±  1.323  us/op
//	SpringDiBenchmark.measure      1  avgt   20   141.145 ±  2.116  us/op
//	SpringDiBenchmark.measure     10  avgt   20  1277.517 ± 28.185  us/op
