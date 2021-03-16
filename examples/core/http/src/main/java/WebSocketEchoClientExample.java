import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.HttpRequest;
import io.activej.http.WebSocket;
import io.activej.http.WebSocket.Message;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

/**
 * A WebSocket client for the {@link WebSocketEchoServerExample}
 * <p>
 * You may write messages to the terminal and receive echoed responses back from the server
 * <p>
 * <b>{@link WebSocketEchoServerExample} should be running prior to launching this example</b>
 */
public final class WebSocketEchoClientExample extends Launcher {

	@Inject
	Eventloop eventloop;

	@Inject
	AsyncHttpClient client;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	AsyncHttpClient client(Eventloop eventloop) {
		return AsyncHttpClient.create(eventloop);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	//[START MAIN]
	@Override
	protected void run() throws Exception {
		String url = args.length != 0 ? args[0] : "ws://127.0.0.1:8080/";
		System.out.println("\nConnecting to WebSocket at: " + url);

		WebSocket webSocket = eventloop.submit(() -> client.webSocketRequest(HttpRequest.get(url))).get();

		Scanner scanIn = new Scanner(System.in);
		while (true) {
			System.out.print("> ");
			String line = scanIn.nextLine();
			if (line.isEmpty()) {
				eventloop.submit(webSocket::close);
				break;
			}
			CompletableFuture<?> future = eventloop.submit(() ->
					webSocket.writeMessage(Message.text(line))
							.then(webSocket::readMessage)
							.whenResult(message -> System.out.println("Response: " + message.getText())));
			future.get();
		}
	}
	//[END MAIN]

	public static void main(String[] args) throws Exception {
		WebSocketEchoClientExample launcher = new WebSocketEchoClientExample();
		launcher.launch(args);
	}
}
