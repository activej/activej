import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.AsyncWebSocket.Message;
import io.activej.http.AsyncWebSocketClient;
import io.activej.http.HttpClient_Reactive;
import io.activej.http.HttpRequest;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public final class WebSocketPingClientExample extends Launcher {
	@Inject
	AsyncWebSocketClient webSocketClient;

	@Inject
	NioReactor reactor;

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	AsyncHttpClient client(NioReactor reactor) {
		return HttpClient_Reactive.create(reactor);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	//[START EXAMPLE]
	@Override
	protected void run() throws ExecutionException, InterruptedException {
		String url = args.length != 0 ? args[0] : "ws://127.0.0.1:8080/";
		System.out.println("\nWeb Socket request: " + url);
		CompletableFuture<?> future = reactor.submit(() -> {
			System.out.println("Sending: Ping");
			return webSocketClient.webSocketRequest(HttpRequest.get(url))
					.then(webSocket -> webSocket.writeMessage(Message.text("Ping"))
							.then(webSocket::readMessage)
							.whenResult(message -> System.out.println("Received: " + message.getText()))
							.whenComplete(webSocket::close));
		});
		future.get();
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		WebSocketPingClientExample example = new WebSocketPingClientExample();
		example.launch(args);
	}
}
