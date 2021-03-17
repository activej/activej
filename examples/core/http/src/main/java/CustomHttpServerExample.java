import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpServer;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

//[START EXAMPLE]
public final class CustomHttpServerExample extends Launcher {
	private static final int PORT = 8080;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	AsyncServlet servlet() {
		return request -> HttpResponse.ok200()
				.withPlainText("Hello from HTTP server");
	}

	@Provides
	@Eager
	AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet).withListenPort(PORT);
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
