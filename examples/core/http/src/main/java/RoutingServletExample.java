import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;

import static io.activej.http.HttpMethod.GET;

public final class RoutingServletExample extends HttpServerLauncher {
	//[START REGION_1]
	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				//[START REGION_2]
				.map(GET, "/", request ->
						HttpResponse.ok200()
								.withHtml("<h1>Go to some pages</h1>" +
										"<a href=\"/path1\"> Path 1 </a><br>" +
										"<a href=\"/path2\"> Path 2 </a><br>" +
										"<a href=\"/path3\"> Path 3 </a>"))
				//[END REGION_2]
				.map(GET, "/path1", request ->
						HttpResponse.ok200()
								.withHtml("<h1>Hello from the first path!</h1>" +
										"<a href=\"/\">Go home</a>"))
				.map(GET, "/path2", request ->
						HttpResponse.ok200()
								.withHtml("<h1>Hello from the second path!</h1>" +
										"<a href=\"/\">Go home</a>"))
				//[START REGION_3]
				.map("/*", request ->
						HttpResponse.ofCode(404)
								.withHtml("<h1>404</h1><p>Path '" + request.getRelativePath() + "' not found</p>" +
										"<a href=\"/\">Go home</a>"));
		//[END REGION_3]
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		Launcher launcher = new RoutingServletExample();
		launcher.launch(args);
	}
}
