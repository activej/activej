import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

import static io.activej.http.HttpMethod.GET;

public final class RoutingServletExample extends HttpServerLauncher {
	//[START REGION_1]
	@Provides
	AsyncServlet servlet(Reactor reactor) {
		return RoutingServlet.builder(reactor)
			//[START REGION_2]
			.with(GET, "/", request ->
				HttpResponse.ok200()
					.withHtml("""
						<h1>Go to some pages</h1>
						<a href="/path1"> Path 1 </a><br>
						<a href="/path2"> Path 2 </a><br>
						<a href="/user/0"> Data for user with ID 0 </a><br>
						<br>
						<a href="/path3"> Non existent </a>
						""")
					.toPromise())
			//[END REGION_2]
			.with(GET, "/path1", request ->
				HttpResponse.ok200()
					.withHtml("""
						<h1>Hello from the first path!</h1>
						<a href="/">Go home</a>
						""")
					.toPromise())
			.with(GET, "/path2", request ->
				HttpResponse.ok200()
					.withHtml("""
						<h1>Hello from the second path!</h1>
						<a href="/">Go home</a>
						""")
					.toPromise())

			//[START REGION_3]
			.with(GET, "/user/:user_id", request -> {
				String userId = request.getPathParameter("user_id");
				return HttpResponse.ok200()
					.withHtml("""
						<h1>You have requested data for user with ID: $userId</h1>
						<h3>Try changing URL after <i>'.../user/'</i> to get data for users with different IDs</h3>
						"""
						.replace("$userId", userId))
					.toPromise();
			})
			//[END REGION_3]

			//[START REGION_4]
			.with("/*", request ->
				HttpResponse.ofCode(404)
					.withHtml("""
						<h1>404</h1><p>Path '$path' not found</p>
						<a href="/">Go home</a>
						"""
						.replace("$path", request.getRelativePath()))
					.toPromise())
			.build();
		//[END REGION_4]
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RoutingServletExample();
		launcher.launch(args);
	}
}
