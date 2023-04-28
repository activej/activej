import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.MultithreadedHttpServerLauncher;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

/**
 * HTTP multithreaded server example.
 * Sends back a greeting and the number of worker which served the connection.
 */
//[START EXAMPLE]
public final class MultithreadedHttpServerExample extends MultithreadedHttpServerLauncher {
	@Provides
	@Worker
	AsyncServlet servlet(@WorkerId int workerId) {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("Hello from worker server #" + workerId + "\n")
				.build();
	}

	public static void main(String[] args) throws Exception {
		MultithreadedHttpServerExample example = new MultithreadedHttpServerExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
