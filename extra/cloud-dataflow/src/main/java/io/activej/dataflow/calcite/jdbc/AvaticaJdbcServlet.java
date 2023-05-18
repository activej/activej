package io.activej.dataflow.calcite.jdbc;

import io.activej.http.AsyncServlet;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.promise.Promise;
import org.apache.calcite.avatica.remote.JsonService;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;

public final class AvaticaJdbcServlet implements AsyncServlet {
	private final Executor executor;
	private final JsonService jsonService;

	private AvaticaJdbcServlet(Executor executor, JsonService jsonService) {
		this.executor = executor;
		this.jsonService = jsonService;
	}

	public static AvaticaJdbcServlet create(Executor executor, JsonService jsonService) {
		return new AvaticaJdbcServlet(executor, jsonService);
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws Exception {
		return request.loadBody()
				.then(body -> {
					String bodyString = body.getString(StandardCharsets.UTF_8);
					return Promise.ofBlocking(executor, () -> jsonService.apply(bodyString))
							.map(responseString -> HttpResponse.ok200()
									.withJson(responseString)
									.build());
				});
	}
}
