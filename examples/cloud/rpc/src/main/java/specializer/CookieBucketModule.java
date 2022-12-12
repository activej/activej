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

	public record Sugar(String name, float weight) {}

	public record Butter(String name, float weight) {}

	public record Flour(String name, float weight) {}

	public record Pastry(Sugar sugar, Butter butter, Flour flour) {}

	public record Cookie1(Pastry pastry) {}

	public record Cookie2(Pastry pastry) {}

	public record Cookie3(Pastry pastry) {}

	public record Cookie4(Pastry pastry) {}

	public record Cookie5(Pastry pastry) {}

	public record Cookie6(Pastry pastry) {}

	public record CookieBucket(Cookie1 c1, Cookie2 c2,
							   Cookie3 c3, Cookie4 c4,
							   Cookie5 c5, Cookie6 c6) {
	}
}
