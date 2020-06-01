import io.activej.di.Injector;
import io.activej.di.annotation.Provides;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.launchers.http.MultithreadedHttpServerLauncher;
import io.activej.worker.Worker;
import io.activej.worker.WorkerId;

/**
 * HTTP multithreaded server example.
 * Sends back a greeting and the number of worker which served the connection.
 */
//[START EXAMPLE]
public final class MultithreadedHttpServerExample extends MultithreadedHttpServerLauncher {
	@Provides
	@Worker
	AsyncServlet servlet(@WorkerId int workerId) {
		return request -> HttpResponse.ok200()
				.withPlainText("Hello from worker server #" + workerId + "\n");
	}

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		MultithreadedHttpServerExample example = new MultithreadedHttpServerExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
