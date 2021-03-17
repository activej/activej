import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.http.StaticServlet;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.activej.http.AsyncServletDecorator.*;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class ServletDecoratorExample extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START REGION_1]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return loadBody().serve(
				RoutingServlet.create()
						.map(GET, "/", StaticServlet.ofClassPath(executor, "static/wrapper")
								.withMappingTo("page.html"))
						.map(POST, "/", request -> {
							String text = request.getPostParameter("text");
							if (text == null) {
								return HttpResponse.redirect302("/");
							}
							return HttpResponse.ok200().withPlainText("Message: " + text);
						})
						.map(GET, "/failPage", request -> {
							throw new RuntimeException("fail");
						})
						.then(catchRuntimeExceptions())
						.then(mapException(e -> HttpResponse.ofCode(404).withPlainText("Error: " + e))));
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		ServletDecoratorExample launcher = new ServletDecoratorExample();
		launcher.launch(args);
	}
}
