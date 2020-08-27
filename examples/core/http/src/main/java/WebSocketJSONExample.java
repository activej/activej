import io.activej.codec.StructuredCodec;
import io.activej.codec.json.JsonUtils;
import io.activej.common.exception.parse.ParseException;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpRequest;
import io.activej.http.RoutingServlet;
import io.activej.http.WebSocket.Message;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.service.ServiceGraphModule;

import static io.activej.codec.StructuredCodecs.*;

public final class WebSocketJSONExample {
	private static final StructuredCodec<Request> REQUEST_CODEC = object(Request::new,
			"left operand", Request::getLeftOperand, DOUBLE_CODEC,
			"right operand", Request::getRightOperand, DOUBLE_CODEC,
			"operator", Request::getOperator, ofEnum(Operator.class));
	private static final StructuredCodec<Response> RESPONSE_CODEC = object(Response::new,
			"result", Response::getResult, DOUBLE_CODEC);

	public static final class WebSocketJSONServer extends HttpServerLauncher {
		@Provides
		AsyncServlet servlet() {
			return RoutingServlet.create()
					.mapWebSocket("/", webSocket -> webSocket.messageReadChannel()
							.map(message -> {
								String requestJson = message.getText();
								Request request;
								try {
									request = JsonUtils.fromJson(REQUEST_CODEC, requestJson);
								} catch (ParseException e) {
									throw new AssertionError(e);
								}
								return processRequest(request);
							})
							.map(response -> JsonUtils.toJson(RESPONSE_CODEC, response))
							.map(Message::text)
							.streamTo(webSocket.messageWriteChannel())
					);
		}

		private static Response processRequest(Request request) {
			double left = request.getLeftOperand();
			double right = request.getRightOperand();
			double result;
			switch (request.getOperator()) {
				case ADD:
					result = left + right;
					break;
				case SUB:
					result = left - right;
					break;
				case MUL:
					result = left * right;
					break;
				case DIV:
					result = left / right;
					break;
				default:
					throw new AssertionError();
			}
			return new Response(result);
		}

		public static void main(String[] args) throws Exception {
			new WebSocketJSONServer().launch(args);
		}
	}

	public static final class WebSocketJSONClient extends Launcher {
		private static final int PORT = 8080;

		@Inject
		AsyncHttpClient client;

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
		protected void run() throws Exception {
			eventloop.submit(() -> client.webSocketRequest(HttpRequest.webSocket("ws://127.0.0.1:" + PORT))
					.then(webSocket -> {
						System.out.println("Sending request for '3 + 5'");
						Request addRequest = new Request(3, 5, Operator.ADD);
						return webSocket.writeMessage(Message.text(JsonUtils.toJson(REQUEST_CODEC, addRequest)))
								.then(webSocket::readMessage)
								.map(WebSocketJSONClient::decode)
								.then(response -> {
									System.out.println("Received response: '" + response.getResult() + '\'');
									System.out.println("Sending request for '120 - 12'");
									Request subRequest = new Request(100, 12, Operator.SUB);
									return webSocket.writeMessage(Message.text(JsonUtils.toJson(REQUEST_CODEC, subRequest)));
								})
								.then(webSocket::readMessage)
								.map(WebSocketJSONClient::decode)
								.then(response -> {
									System.out.println("Received response: '" + response.getResult() + '\'');
									System.out.println("Sending request for '43 * 3'");
									Request mulRequest = new Request(43, 3, Operator.MUL);
									return webSocket.writeMessage(Message.text(JsonUtils.toJson(REQUEST_CODEC, mulRequest)));
								})
								.then(webSocket::readMessage)
								.map(WebSocketJSONClient::decode)
								.then(response -> {
									System.out.println("Received response: '" + response.getResult() + '\'');
									System.out.println("Sending request for '500 / 43'");
									Request divRequest = new Request(500, 43, Operator.DIV);
									return webSocket.writeMessage(Message.text(JsonUtils.toJson(REQUEST_CODEC, divRequest)));
								})
								.then(webSocket::readMessage)
								.map(WebSocketJSONClient::decode)
								.whenResult(response -> {
									System.out.println("Received response: '" + response.getResult() + '\'');
									webSocket.close();
								});
					})).get();
		}

		private static Response decode(Message message) {
			try {
				return JsonUtils.fromJson(RESPONSE_CODEC, message.getText());
			} catch (ParseException e) {
				throw new AssertionError();
			}
		}

		public static void main(String[] args) throws Exception {
			new WebSocketJSONClient().launch(args);
		}
	}

	// region POJOs
	private enum Operator {
		ADD, SUB, DIV, MUL
	}

	private static final class Request {
		private final double leftOperand;
		private final double rightOperand;
		private final Operator operator;

		private Request(double leftOperand, double rightOperand, Operator operator) {
			this.leftOperand = leftOperand;
			this.rightOperand = rightOperand;
			this.operator = operator;
		}

		public double getLeftOperand() {
			return leftOperand;
		}

		public double getRightOperand() {
			return rightOperand;
		}

		public Operator getOperator() {
			return operator;
		}
	}

	private static final class Response {
		private final double result;

		private Response(double result) {
			this.result = result;
		}

		public double getResult() {
			return result;
		}
	}
	// endregion

}
