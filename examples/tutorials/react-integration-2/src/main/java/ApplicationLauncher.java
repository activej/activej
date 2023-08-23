import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.http.StaticServlet;
import io.activej.http.loader.IStaticLoader;
import io.activej.inject.annotation.Provides;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;
import io.activej.json.JsonKeyCodec;
import io.activej.json.JsonUtils;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

import java.util.Map;
import java.util.concurrent.Executor;

import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START REGION_1]
public final class ApplicationLauncher extends HttpServerLauncher {
	@Provides
	RecordDAO recordRepo() {
		return new RecordImplDAO();
	}

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}
	//[END REGION_1]

	//[START REGION_2]
	@Provides
	IStaticLoader staticLoader(Reactor reactor, Executor executor) {
		return IStaticLoader.ofClassPath(reactor, executor, "build/");
	}

	@Provides
	JsonCodec<Record> recordJsonCodec() {
		return JsonCodecs.ofObject(Record::new,
			"title", Record::getTitle, JsonCodecs.ofString(),
			"plans", Record::getPlans, JsonCodecs.ofList(
				JsonCodecs.ofObject(Plan::new,
					"text", Plan::getText, JsonCodecs.ofString(),
					"isComplete", Plan::isComplete, JsonCodecs.ofBoolean())
			));
	}

	@Provides
	AsyncServlet servlet(Reactor reactor, IStaticLoader staticLoader, RecordDAO recordDAO, JsonCodec<Record> recordJsonCodec) {
		JsonCodec<Map<Integer, Record>> mapJsonCodec = JsonCodecs.ofMap(JsonKeyCodec.ofNumberKey(Integer.class), recordJsonCodec);
		return RoutingServlet.builder(reactor)
			.with("/*", StaticServlet.builder(reactor, staticLoader)
				.withIndexHtml()
				.build())
			//[END REGION_2]
			//[START REGION_3]
			.with(POST, "/add", request -> request.loadBody()
				.then($ -> {
					ByteBuf body = request.getBody();
					try {
						byte[] bodyBytes = body.getArray();
						Record record = JsonUtils.fromJsonBytes(recordJsonCodec, bodyBytes);
						recordDAO.add(record);
						return HttpResponse.ok200().toPromise();
					} catch (MalformedDataException e) {
						return HttpResponse.ofCode(400).toPromise();
					}
				}))
			.with(GET, "/get/all", request -> {
				Map<Integer, Record> records = recordDAO.findAll();
				return HttpResponse.ok200()
					.withJson(JsonUtils.toJson(mapJsonCodec, records))
					.toPromise();
			})
			//[START REGION_4]
			.with(GET, "/delete/:recordId", request -> {
				int id = parseInt(request.getPathParameter("recordId"));
				recordDAO.delete(id);
				return HttpResponse.ok200().toPromise();
			})
			//[END REGION_4]
			.with(GET, "/toggle/:recordId/:planId", request -> {
				int id = parseInt(request.getPathParameter("recordId"));
				int planId = parseInt(request.getPathParameter("planId"));

				Record record = recordDAO.find(id);
				Plan plan = record.getPlans().get(planId);
				plan.toggle();
				return HttpResponse.ok200().toPromise();
			})
			.build();
		//[END REGION_3]
	}

	//[START REGION_5]
	public static void main(String[] args) throws Exception {
		ApplicationLauncher launcher = new ApplicationLauncher();
		launcher.launch(args);
	}
	//[END REGION_5]
}
