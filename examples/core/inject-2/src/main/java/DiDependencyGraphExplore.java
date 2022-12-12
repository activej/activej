import io.activej.inject.Injector;
import io.activej.inject.Scope;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.util.Utils;

public class DiDependencyGraphExplore {
	static class Sugar {
		private final String name;
		private final float weight;

		@Inject
		public Sugar() {
			this.name = "WhiteSugar";
			this.weight = 10.f;
		}

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

	static class Cookie {
		private final Pastry pastry;

		@Inject
		Cookie(Pastry pastry) {
			this.pastry = pastry;
		}

		public Pastry getPastry() {
			return pastry;
		}

		@Override
		public String toString() {
			return "Cookie {" + pastry + '}';
		}
	}

	public static void main(String[] args) {
		AbstractModule cookbook = new AbstractModule() {
			@Override
			protected void configure() {
				bind(Cookie.class).to(Cookie::new, Pastry.class);
			}

			@Provides
			@OrderScope
			Sugar sugar() {return new Sugar("WhiteSugar", 10.f);}

			@Provides
			@OrderScope
			Butter butter() {return new Butter("PerfectButter", 20.0f);}

			@Provides
			@OrderScope
			Flour flour() {return new Flour("GoodFlour", 100.0f);}

			@Provides
			@OrderScope
			Pastry pastry(Sugar sugar, Butter butter, Flour flour) {
				return new Pastry(sugar, butter, flour);
			}

			@Provides
			@OrderScope
			Cookie cookie(Pastry pastry) {
				return new Cookie(pastry);
			}
		};
		Injector injector = Injector.of(cookbook);

		/// peekInstance, hasInstance and getInstance instance.
		//[START REGION_1]
		Cookie cookie1 = injector.peekInstance(Cookie.class);
		System.out.println("Instance is present in injector before 'get' : " + injector.hasInstance(Cookie.class));
		System.out.println("Instance before get : " + cookie1);

		Cookie cookie = injector.getInstance(Cookie.class);

		Cookie cookie2 = injector.peekInstance(Cookie.class);
		System.out.println("Instance is present in injector after 'get' : " + injector.hasInstance(Cookie.class));
		System.out.println("Instance after get : " + cookie2);
		System.out.println();    /// created instance check.
		System.out.println("Instances are same : " + cookie.equals(cookie2));
		//[END REGION_1]
		System.out.println();
		System.out.println("============================ ");
		System.out.println();

		/// parent injectors
		//[START REGION_2]
		final Scope ORDER_SCOPE = Scope.of(OrderScope.class);

		System.out.println("Parent injector, before entering scope : " + injector);

		Injector subInjector = injector.enterScope(ORDER_SCOPE);
		System.out.println("Parent injector, after entering scope : " + subInjector.getParent());
		System.out.println("Parent injector is 'injector' : " + injector.equals(subInjector.getParent()));
		//[END REGION_2]
		System.out.println();
		System.out.println("============================ ");
		System.out.println();

		/// bindings check
		// FYI: getBinding().toString() gives us a dependencies of current binding.
		//[START REGION_3]
		System.out.println("Pastry binding check : " + subInjector.getBinding(Pastry.class));
		//[END REGION_3]
		System.out.println();
		System.out.println("============================ ");
		System.out.println();

		// graphviz visualization.
		//[START REGION_4]
		Utils.printGraphVizGraph(subInjector.getBindingsTrie());
		//[END REGION_4]
	}
}
