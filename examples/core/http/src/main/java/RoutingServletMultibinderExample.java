import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.Multibinder;
import io.activej.inject.binding.Multibinders;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;

/**
 * An example of combining {@link RoutingServlet}s provided in multiple DI modules.
 * <p>
 * Servlets are provided via the same {@link Key} ({@link RoutingServlet}).
 * So, to resolve DI conflicts we may use {@link Multibinder} which combines all the
 * conflicting {@link RoutingServlet}s into a single {@link RoutingServlet} which contains all the routes
 * mapped by other servlets.
 * If there are conflicting routes mapped in different modules, a runtime exception would be thrown
 * <p>
 * You may test routes either by accessing mapped routes via browser or by issuing {@code curl} commands:
 * <ul>
 *     <li>{@code curl http://localhost:8080}</li>
 *     <li>{@code curl http://localhost:8080/a}</li>
 *     <li>{@code curl http://localhost:8080/a/b}</li>
 *     <li>{@code curl http://localhost:8080/a/c}</li>
 *     <li>{@code curl http://localhost:8080/b}</li>
 *     <li>{@code curl http://localhost:8080/b/a}</li>
 *     <li>{@code curl http://localhost:8080/b/c}</li>
 *     <li>{@code curl http://localhost:8080/d}</li>
 *     <li>{@code curl -X POST http://localhost:8080/d}</li>
 * </ul>
 */

public final class RoutingServletMultibinderExample extends HttpServerLauncher {

	//[START MULTIBINDER]
	public static final Multibinder<RoutingServlet> SERVLET_MULTIBINDER = Multibinders.ofBinaryOperator((servlet1, servlet2) ->
			servlet1.merge(servlet2));
	//[END MULTIBINDER]

	//[START MAIN_MODULE]
	@Override
	protected Module getBusinessLogicModule() {
		return Modules.combine(
				new ModuleA(),
				new ModuleB(),
				new ModuleC(),
				ModuleBuilder.create()
						.multibind(Key.of(RoutingServlet.class), SERVLET_MULTIBINDER)
						.build()
		);
	}
	//[END MAIN_MODULE]

	public static void main(String[] args) throws Exception {
		new RoutingServletMultibinderExample().launch(args);
	}

	//[START MODULE_A]
	private static final class ModuleA extends AbstractModule {
		@Provides
		RoutingServlet servlet(Reactor reactor) {
			return RoutingServlet.create(reactor)
					.map(GET, "/a", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/a' path\n")
							.build())
					.map(GET, "/b", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/b' path\n")
							.build())
					.map(GET, "/", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/' path\n")
							.build());
		}
	}
	//[END MODULE_A]

	//[START MODULE_B]
	private static final class ModuleB extends AbstractModule {
		@Provides
		RoutingServlet servlet(Reactor reactor) {
			return RoutingServlet.create(reactor)
					.map(GET, "/a/b", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/a/b' path\n")
							.build())
					.map(GET, "/b/a", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/b/a' path\n")
							.build())
					.map(GET, "/d", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/d' path\n")
							.build());
		}
	}
	//[END MODULE_B]

	//[START MODULE_C]
	private static final class ModuleC extends AbstractModule {
		@Provides
		RoutingServlet servlet(Reactor reactor) {
			return RoutingServlet.create(reactor)
					.map(GET, "/a/c", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/a/c' path\n")
							.build())
					.map(GET, "/b/c", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from '/b/c' path\n")
							.build())
					.map(POST, "/d", request -> HttpResponse.Builder.ok200()
							.withPlainText("Hello from POST '/d' path\n")
							.build());
		}
	}
	//[END MODULE_C]
}
