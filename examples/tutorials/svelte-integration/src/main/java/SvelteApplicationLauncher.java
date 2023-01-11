import io.activej.http.AsyncServlet;
import io.activej.http.Servlet_Static;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START EXAMPLE]
public final class SvelteApplicationLauncher extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	@Provides
	AsyncServlet servlet(Executor executor) {
		return Servlet_Static.ofClassPath(executor, "public")
				.withIndexHtml();
	}

	public static void main(String[] args) throws Exception {
		SvelteApplicationLauncher launcher = new SvelteApplicationLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
