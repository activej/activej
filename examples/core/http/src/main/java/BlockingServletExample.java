import io.activej.di.Injector;
import io.activej.di.annotation.Provides;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newCachedThreadPool;

public final class BlockingServletExample extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newCachedThreadPool();
	}

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return RoutingServlet.create()
				.map("/", request -> HttpResponse.ok200()
						.withHtml("<a href='hardWork'>Do hard work</a>"))
				.map("/hardWork", AsyncServlet.ofBlocking(executor, request -> {
					Thread.sleep(2000); //Hard work
					return HttpResponse.ok200()
							.withHtml("Hard work is done");
				}));
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		Launcher launcher = new BlockingServletExample();
		launcher.launch(args);
	}
}
