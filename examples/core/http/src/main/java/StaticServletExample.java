import io.activej.http.AsyncServlet;
import io.activej.http.Servlet_Static;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class StaticServletExample extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet(Reactor reactor, Executor executor) {
		return Servlet_Static.ofClassPath(reactor, executor, "static/site")
				.withIndexHtml();
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
