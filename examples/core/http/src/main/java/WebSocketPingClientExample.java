import io.activej.eventloop.Eventloop;
import io.activej.http.HttpClient;
import io.activej.http.HttpRequest;
import io.activej.http.IHttpClient;
import io.activej.http.IWebSocket.Message;
import io.activej.http.IWebSocketClient;
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
	IWebSocketClient webSocketClient;

	@Inject
	NioReactor reactor;

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	IHttpClient client(NioReactor reactor) {
		return HttpClient.create(reactor);
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
