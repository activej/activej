import io.activej.http.AsyncServlet;
import io.activej.http.HttpHeaders;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

/**
 * An example of setting routes based on Host
 * <p>
 * You may test server behaviour by issuing {@code curl} commands:
 * <ul>
 *     <li>{@code curl -H "Host: test.com" http://localhost:8080}</li>
 *     <li>{@code curl -H "Host: example.com" http://localhost:8080}</li>
 * </ul>
 */
public final class HostRoutingExample extends HttpServerLauncher {
	private static final String TEST_HOST = "test.com";
	private static final String EXAMPLE_HOST = "example.com";

	@Provides
	AsyncServlet mainServlet(Reactor reactor, @Named("Test") AsyncServlet testServlet, @Named("Example") AsyncServlet exampleServlet) {
		return RoutingServlet.create(reactor)
				.map("/*", request -> {
					String hostHeader = request.getHeader(HttpHeaders.HOST);
					if (hostHeader == null) {
						return HttpResponse.builder(400)
								.withPlainText("Host header is missing")
								.toPromise();
					}
					if (hostHeader.equals(TEST_HOST)) {
						return testServlet.serve(request);
					}
					if (hostHeader.equals(EXAMPLE_HOST)) {
						return exampleServlet.serve(request);
					}
					return HttpResponse.builder(400)
							.withPlainText("Unknown host")
							.toPromise();
				});
	}

	@Provides
	@Named("Test")
	AsyncServlet testServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("This page is served on test.com\n")
				.toPromise();
	}

	@Provides
	@Named("Example")
	AsyncServlet exampleServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("This page is served on example.com\n")
				.toPromise();
	}

	public static void main(String[] args) throws Exception {
		new HostRoutingExample().launch(args);
	}
}
