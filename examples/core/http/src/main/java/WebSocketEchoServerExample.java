import io.activej.http.AsyncServlet;
import io.activej.http.RoutingServlet;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

public final class WebSocketEchoServerExample extends HttpServerLauncher {

	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.mapWebSocket("/", webSocket -> webSocket.messageReadChannel()
						.streamTo(webSocket.messageWriteChannel()));
	}

	public static void main(String[] args) throws Exception {
		WebSocketEchoServerExample launcher = new WebSocketEchoServerExample();
		launcher.launch(args);
	}
}
