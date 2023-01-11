import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.Servlet_Routing;
import io.activej.http.Servlet_Static;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class HttpRequestParametersExample extends HttpServerLauncher {
	private static final String RESOURCE_DIR = "static/query";

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START REGION_1]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return Servlet_Routing.create()
				.map(POST, "/hello", request -> request.loadBody()
						.map($ -> {
							String name = request.getPostParameters().get("name");
							return HttpResponse.ok200()
									.withHtml("<h1><center>Hello from POST, " + name + "!</center></h1>");
						}))
				.map(GET, "/hello", request -> {
					String name = request.getQueryParameter("name");
					return HttpResponse.ok200()
							.withHtml("<h1><center>Hello from GET, " + name + "!</center></h1>");
				})
				.map("/*", Servlet_Static.ofClassPath(executor, RESOURCE_DIR)
						.withIndexHtml());
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpRequestParametersExample();
		launcher.launch(args);
	}
}
