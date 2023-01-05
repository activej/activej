import io.activej.http.AsyncServlet;
import io.activej.http.RoutingServlet;
import io.activej.http.AsyncWebSocket.Message;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

public final class WebSocketPongServerExample extends HttpServerLauncher {

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.mapWebSocket("/", webSocket -> webSocket.readMessage()
						.whenResult(message -> System.out.println("Received:" + message.getText()))
						.then(() -> webSocket.writeMessage(Message.text("Pong")))
						.whenComplete(webSocket::close));
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		WebSocketPongServerExample launcher = new WebSocketPongServerExample();
		launcher.launch(args);
	}
}
