import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

/**
 * An example of setting routes based on Cookies.
 * <p>
 * You may test server behaviour by issuing {@code curl} commands:
 * <ul>
 *     <li>{@code curl --cookie "SERVLET_NUMBER=1" http://localhost:8080}</li>
 *     <li>{@code curl --cookie "SERVLET_NUMBER=2" http://localhost:8080}</li>
 * </ul>
 */
public final class CookieRoutingExample extends HttpServerLauncher {

	private static final String COOKIE = "SERVLET_NUMBER";

	@Provides
	AsyncServlet mainServlet(Reactor reactor, @Named("First") AsyncServlet firstServlet, @Named("Second") AsyncServlet secondServlet) {
		return RoutingServlet.create(reactor)
				.map("/*", request -> {
					String servletNumberCookie = request.getCookie(COOKIE);
					if (servletNumberCookie == null) {
						return HttpResponse.builder(400)
								.withPlainText("Cookie '" + COOKIE + "' is missing")
								.build();
					}
					if ("1".equals(servletNumberCookie)) {
						return firstServlet.serve(request);
					}
					if ("2".equals(servletNumberCookie)) {
						return secondServlet.serve(request);
					}
					return HttpResponse.builder(400)
							.withPlainText("Unknown servlet number")
							.build();
				});
	}

	@Provides
	@Named("First")
	AsyncServlet firstServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("This is servlet #1\n")
				.build();
	}

	@Provides
	@Named("Second")
	AsyncServlet secondServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("This is servlet #2\n")
				.build();
	}

	public static void main(String[] args) throws Exception {
		new CookieRoutingExample().launch(args);
	}
}
