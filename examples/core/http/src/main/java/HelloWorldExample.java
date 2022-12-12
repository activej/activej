import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpServer;
import io.activej.http.HttpResponse;

import java.io.IOException;

public final class HelloWorldExample {
	//[START REGION_1]

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create();
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
						request -> HttpResponse.ok200()
								.withPlainText("Hello world!"))
				.withListenPort(8080);

		server.listen();

		System.out.println("Server is running");
		System.out.println("You can connect from browser by visiting 'http://localhost:8080/'");

		eventloop.run();
	}
	//[END REGION_1]
}
