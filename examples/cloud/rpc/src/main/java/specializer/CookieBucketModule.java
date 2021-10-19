package specializer;

import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;

public final class CookieBucketModule extends AbstractModule {

	public static Module create() {
		return ModuleBuilder.create()
				.bind(Sugar.class).to(() -> new Sugar("WhiteSugar", 10.f)).in(RpcRequestScope.class)
				.bind(Butter.class).to(() -> new Butter("PerfectButter", 20.0f)).in(RpcRequestScope.class)
				.bind(Flour.class).to(() -> new Flour("GoodFlour", 100.0f)).in(RpcRequestScope.class)
				.bind(Pastry.class).to(Pastry::new, Sugar.class, Butter.class, Flour.class).in(RpcRequestScope.class)
				.bind(Cookie1.class).to(Cookie1::new, Pastry.class).in(RpcRequestScope.class)
				.bind(Cookie2.class).to(Cookie2::new, Pastry.class).in(RpcRequestScope.class)
				.bind(Cookie3.class).to(Cookie3::new, Pastry.class).in(RpcRequestScope.class)
				.bind(Cookie4.class).to(Cookie4::new, Pastry.class).in(RpcRequestScope.class)
				.bind(Cookie5.class).to(Cookie5::new, Pastry.class).in(RpcRequestScope.class)
				.bind(Cookie6.class).to(Cookie6::new, Pastry.class).in(RpcRequestScope.class)
				.bind(CookieBucket.class).to(CookieBucket::new, Cookie1.class, Cookie2.class,
						Cookie3.class, Cookie4.class, Cookie5.class, Cookie6.class).in(RpcRequestScope.class)
				.build();
	}

	public static class Sugar {
		public final String name;
		public final float weight;

		public Sugar(String name, float weight) {
			this.name = name;
			this.weight = weight;
		}
	}

	public static class Butter {
		public final float weight;
		public final String name;

		public Butter(String name, float weight) {
			this.weight = weight;
			this.name = name;
		}
	}

	public static class Flour {
		public final float weight;
		public final String name;

		public Flour(String name, float weight) {
			this.weight = weight;
			this.name = name;
		}
	}

	public static class Pastry {
		public final Sugar sugar;
		public final Butter butter;
		public final Flour flour;

		public Pastry(Sugar sugar, Butter butter, Flour flour) {
			this.sugar = sugar;
			this.butter = butter;
			this.flour = flour;
		}
	}

	public static class Cookie1 {
		public final Pastry pastry;

		public Cookie1(Pastry pastry) {
			this.pastry = pastry;
		}
	}

	public static class Cookie2 {
		public final Pastry pastry;

		public Cookie2(Pastry pastry) {
			this.pastry = pastry;
		}
	}

	public static class Cookie3 {
		public final Pastry pastry;

		public Cookie3(Pastry pastry) {
			this.pastry = pastry;
		}
	}

	public static class Cookie4 {
		public final Pastry pastry;

		public Cookie4(Pastry pastry) {
			this.pastry = pastry;
		}
	}

	public static class Cookie5 {
		private final Pastry pastry;

		public Cookie5(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}
	}

	public static class Cookie6 {
		public final Pastry pastry;

		public Cookie6(Pastry pastry) {
			this.pastry = pastry;
		}
	}

	public static class CookieBucket {
		public final Cookie1 c1;
		public final Cookie2 c2;
		public final Cookie3 c3;
		public final Cookie4 c4;
		public final Cookie5 c5;
		public final Cookie6 c6;

		public CookieBucket(Cookie1 c1, Cookie2 c2, Cookie3 c3,
				Cookie4 c4, Cookie5 c5, Cookie6 c6) {
			this.c1 = c1;
			this.c2 = c2;
			this.c3 = c3;
			this.c4 = c4;
			this.c5 = c5;
			this.c6 = c6;
		}
	}
}
