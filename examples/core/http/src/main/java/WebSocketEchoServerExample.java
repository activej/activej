import io.activej.http.AsyncServlet;
import io.activej.http.AsyncWebSocket.Message;
import io.activej.http.AsyncWebSocket.Message.MessageType;
import io.activej.http.Servlet_Routing;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A WebSocket server that sends echoed messages back to the WebSocket client
 * <p>
 * Messages are logged upon being received
 * <p>
 * To communicate with the server, you may use either {@link WebSocketEchoClientExample}
 * or any other WebSocket client
 */
public final class WebSocketEchoServerExample extends HttpServerLauncher {

	//[START MAIN]
	@Provides
	AsyncServlet servlet() {
		return Servlet_Routing.create()
				.mapWebSocket("/", webSocket -> webSocket.messageReadChannel()
						.peek(this::logMessage)
						.streamTo(webSocket.messageWriteChannel()));
	}
	//[END MAIN]

	private void logMessage(Message message) {
		String msg;
		if (message.getType() == MessageType.TEXT) {
			msg = message.getText();
		} else {
			msg = message.getBuf().getString(UTF_8);
		}
		logger.info("Received message: {}", msg);
	}

	public static void main(String[] args) throws Exception {
		WebSocketEchoServerExample launcher = new WebSocketEchoServerExample();
		launcher.launch(args);
	}
}
