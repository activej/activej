import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.http.StaticServlet;
import io.activej.http.loader.IStaticLoader;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

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
	IStaticLoader staticLoader(Reactor reactor, Executor executor) {
		return IStaticLoader.ofClassPath(reactor, executor, RESOURCE_DIR);
	}

	@Provides
	AsyncServlet servlet(Reactor reactor, IStaticLoader staticLoader) {
		return RoutingServlet.create(reactor)
				.map(POST, "/hello", request -> request.loadBody()
						.map($ -> {
							String name = request.getPostParameters().get("name");
							return HttpResponse.Builder.ok200()
									.withHtml("<h1><center>Hello from POST, " + name + "!</center></h1>")
									.build();
						}))
				.map(GET, "/hello", request -> {
					String name = request.getQueryParameter("name");
					return HttpResponse.Builder.ok200()
							.withHtml("<h1><center>Hello from GET, " + name + "!</center></h1>")
							.toPromise();
				})
				.map("/*", StaticServlet.builder(reactor, staticLoader)
						.withIndexHtml()
						.build());
	}
	//[END REGION_1]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpRequestParametersExample();
		launcher.launch(args);
	}
}
