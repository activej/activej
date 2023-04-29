import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

//[START EXAMPLE]
public final class CustomHttpServerExample extends Launcher {
	private static final int PORT = 8080;

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	AsyncServlet servlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("Hello from HTTP server")
				.toPromise();
	}

	@Provides
	@Eager
	HttpServer server(NioReactor reactor, AsyncServlet servlet) {
		return HttpServer.builder(reactor, servlet)
				.withListenPort(PORT)
				.build();
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is now available at http://localhost:" + PORT);
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CustomHttpServerExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
