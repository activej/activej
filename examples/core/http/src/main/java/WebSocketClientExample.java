import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.HttpRequest;
import io.activej.http.WebSocket.Message;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promises;
import io.activej.service.ServiceGraphModule;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public final class WebSocketClientExample extends Launcher {
	@Inject
	AsyncHttpClient httpClient;

	@Inject
	Eventloop eventloop;

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

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		String url = args.length != 0 ? args[0] : "ws://127.0.0.1:8080/";
		System.out.println("\nWeb Socket request: " + url);
		CompletableFuture<?> future = eventloop.submit(() ->
				httpClient.webSocketRequest(HttpRequest.webSocket(url))
						.then(webSocket -> {
							ChannelSupplier.of("Hello", "This", "Messages", "Should", "Be", "Echoed", "Via", "Web", "Socket")
									.mapAsync(message -> Promises.delay(Duration.ofSeconds(1), message))
									.peek(message -> System.out.println("Sending: " + message))
									.map(Message::text)
									.streamTo(webSocket.messageWriteChannel());
							return webSocket.messageReadChannel()
									.streamTo(ChannelConsumer.ofConsumer(message -> System.out.println("Received: " + message.getText())));
						}));

		future.get();
	}

	public static void main(String[] args) throws Exception {
		WebSocketClientExample example = new WebSocketClientExample();
		example.launch(args);
	}
}
