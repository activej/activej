import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.Servlet_Routing;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;

import static io.activej.http.HttpMethod.GET;

public final class RoutingServletExample extends HttpServerLauncher {
	//[START REGION_1]
	@Provides
	AsyncServlet servlet() {
		return Servlet_Routing.create()
				//[START REGION_2]
				.map(GET, "/", request ->
						HttpResponse.ok200()
								.withHtml("<h1>Go to some pages</h1>" +
										"<a href=\"/path1\"> Path 1 </a><br>" +
										"<a href=\"/path2\"> Path 2 </a><br>" +
										"<a href=\"/user/0\"> Data for user with ID 0 </a><br>" +
										"<br>" +
										"<a href=\"/path3\"> Non existent </a>"))
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
				.map(GET, "/user/:user_id", request -> {
					String userId = request.getPathParameter("user_id");
					return HttpResponse.ok200()
							.withHtml("<h1>You have requested data for user with ID: " + userId + "</h1>" +
									"<h3>Try changing URL after <i>'.../user/'</i> to get data for users with different IDs</h3>");
				})
				//[END REGION_3]

				//[START REGION_4]
				.map("/*", request ->
						HttpResponse.ofCode(404)
								.withHtml("<h1>404</h1><p>Path '" + request.getRelativePath() + "' not found</p>" +
										"<a href=\"/\">Go home</a>"));
		//[END REGION_4]
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RoutingServletExample();
		launcher.launch(args);
	}
}
