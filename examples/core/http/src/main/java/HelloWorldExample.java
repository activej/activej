import io.activej.eventloop.Eventloop;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;

import java.io.IOException;

public final class HelloWorldExample {
	//[START REGION_1]

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create();
		HttpServer server = HttpServer.builder(eventloop,
						request -> HttpResponse.Builder.ok200()
								.withPlainText("Hello world!")
								.toPromise())
				.withListenPort(8080)
				.build();

		server.listen();

		System.out.println("Server is running");
		System.out.println("You can connect from browser by visiting 'http://localhost:8080/'");

		eventloop.run();
	}
	//[END REGION_1]
}
