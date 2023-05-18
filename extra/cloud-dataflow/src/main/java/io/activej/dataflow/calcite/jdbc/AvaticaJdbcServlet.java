package io.activej.dataflow.calcite.jdbc;

import io.activej.http.AsyncServlet;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.promise.Promise;
import org.apache.calcite.avatica.remote.Handler;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;

public final class AvaticaJdbcServlet implements AsyncServlet {
	private final Executor executor;
	private final Handler<String> handler;

	private AvaticaJdbcServlet(Executor executor, Handler<String> handler) {
		this.executor = executor;
		this.handler = handler;
	}

	public static AvaticaJdbcServlet create(Executor executor, Handler<String> handler) {
		return new AvaticaJdbcServlet(executor, handler);
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws Exception {
		return request.loadBody()
				.then(body -> {
					String bodyString = body.getString(StandardCharsets.UTF_8);
					return Promise.ofBlocking(executor, () -> handler.apply(bodyString));
				})
				.map(responseString -> HttpResponse.ok200()
						.withJson(responseString.getResponse())
						.build());
	}
}
