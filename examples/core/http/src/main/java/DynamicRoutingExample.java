import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

import java.util.concurrent.ThreadLocalRandom;

/**
 * An example of setting routes that change at random.
 * <p>
 * You may test server behaviour by issuing accessing <a href="http://localhost:8080">the server</a> from a browser
 */
public final class DynamicRoutingExample extends HttpServerLauncher {

	@Provides
	AsyncServlet mainServlet(Reactor reactor, @Named("First") AsyncServlet firstServlet, @Named("Second") AsyncServlet secondServlet) {
		return RoutingServlet.create(reactor)
				.map("/*", request -> {
					if (ThreadLocalRandom.current().nextBoolean()) {
						return firstServlet.serve(request);
					} else {
						return secondServlet.serve(request);
					}
				});
	}

	@Provides
	@Named("First")
	AsyncServlet firstServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withHtml(
						"<h1>This page is served by first servlet</h1>" +
								"<h3>Try to reload the page</h3>"
				)
				.build();
	}

	@Provides
	@Named("Second")
	AsyncServlet secondServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withHtml(
						"<h1>This page is served by second servlet</h1>" +
								"<h3>Try to reload the page</h3>"
				)
				.build();
	}

	public static void main(String[] args) throws Exception {
		new DynamicRoutingExample().launch(args);
	}
}
