import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.runtime.Settings;
import io.activej.bytebuf.ByteBuf;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.Servlet_Routing;
import io.activej.http.Servlet_Static;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

import java.io.IOException;
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

	@Provides
	DslJson<?> dslJson() {
		return new DslJson<>(Settings.withRuntime());
	}
	//[END REGION_1]

	//[START REGION_2]
	@Provides
	AsyncServlet servlet(Executor executor, RecordDAO recordDAO, DslJson<?> dslJson) {
		return Servlet_Routing.create()
				.map("/*", Servlet_Static.ofClassPath(executor, "build/")
						.withIndexHtml())
				//[END REGION_2]
				//[START REGION_3]
				.map(POST, "/add", request -> request.loadBody()
						.map($ -> {
							ByteBuf body = request.getBody();
							try {
								byte[] bodyBytes = body.getArray();
								Record record = dslJson.deserialize(Record.class, bodyBytes, bodyBytes.length);
								recordDAO.add(record);
								return HttpResponse.ok200();
							} catch (IOException e) {
								return HttpResponse.ofCode(400);
							}
						}))
				.map(GET, "/get/all", request -> {
					Map<Integer, Record> records = recordDAO.findAll();
					JsonWriter writer = dslJson.newWriter();
					try {
						dslJson.serialize(writer, records);
					} catch (IOException e) {
						throw new AssertionError(e);
					}
					return HttpResponse.ok200()
							.withJson(writer.toString());
				})
				//[START REGION_4]
				.map(GET, "/delete/:recordId", request -> {
					int id = parseInt(request.getPathParameter("recordId"));
					recordDAO.delete(id);
					return HttpResponse.ok200();
				})
				//[END REGION_4]
				.map(GET, "/toggle/:recordId/:planId", request -> {
					int id = parseInt(request.getPathParameter("recordId"));
					int planId = parseInt(request.getPathParameter("planId"));

					Record record = recordDAO.find(id);
					Plan plan = record.getPlans().get(planId);
					plan.toggle();
					return HttpResponse.ok200();
				});
		//[END REGION_3]
	}

	//[START REGION_5]
	public static void main(String[] args) throws Exception {
		ApplicationLauncher launcher = new ApplicationLauncher();
		launcher.launch(args);
	}
	//[END REGION_5]
}
