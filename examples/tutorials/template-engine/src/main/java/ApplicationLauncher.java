import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.util.ByteBufWriter;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.Servlet_Routing;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.promise.Promise;

import java.util.List;
import java.util.Map;

import static io.activej.common.Utils.nonNullElse;
import static io.activej.http.HttpHeaders.REFERER;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;

//[START REGION_1]
public final class ApplicationLauncher extends HttpServerLauncher {

	private static ByteBuf applyTemplate(Mustache mustache, Map<String, Object> scopes) {
		ByteBufWriter writer = new ByteBufWriter();
		mustache.execute(writer, scopes);
		return writer.getBuf();
	}

	@Provides
	PollDao pollRepo() {
		return new PollDaoImpl();
	}

	//[END REGION_1]
	//[START REGION_2]
	@Provides
	AsyncServlet servlet(PollDao pollDao) {
		Mustache singlePollView = new DefaultMustacheFactory().compile("templates/singlePollView.html");
		Mustache singlePollCreate = new DefaultMustacheFactory().compile("templates/singlePollCreate.html");
		Mustache listPolls = new DefaultMustacheFactory().compile("templates/listPolls.html");

		return Servlet_Routing.create()
				.map(GET, "/", request -> HttpResponse.ok200()
						.withBody(applyTemplate(listPolls, Map.of("polls", pollDao.findAll().entrySet()))))
				//[END REGION_2]
				//[START REGION_3]
				.map(GET, "/poll/:id", request -> {
					int id = Integer.parseInt(request.getPathParameter("id"));
					return HttpResponse.ok200()
							.withBody(applyTemplate(singlePollView, Map.of("id", id, "poll", pollDao.find(id))));
				})
				//[END REGION_3]
				//[START REGION_4]
				.map(GET, "/create", request ->
						HttpResponse.ok200()
								.withBody(applyTemplate(singlePollCreate, Map.of())))
				.map(POST, "/vote", request -> request.loadBody()
						.then(() -> {
							Map<String, String> params = request.getPostParameters();
							String option = params.get("option");
							String stringId = params.get("id");
							if (option == null || stringId == null) {
								return Promise.of(HttpResponse.ofCode(401));
							}

							int id = Integer.parseInt(stringId);
							PollDao.Poll question = pollDao.find(id);

							question.vote(option);

							return Promise.of(HttpResponse.redirect302(nonNullElse(request.getHeader(REFERER), "/")));
						}))
				.map(POST, "/add", request -> request.loadBody()
						.map($ -> {
							Map<String, String> params = request.getPostParameters();
							String title = params.get("title");
							String message = params.get("message");

							String option1 = params.get("option1");
							String option2 = params.get("option2");

							int id = pollDao.add(new PollDao.Poll(title, message, List.of(option1, option2)));
							return HttpResponse.redirect302("poll/" + id);
						}))
				.map(POST, "/delete", request -> request.loadBody()
						.then(() -> {
							Map<String, String> params = request.getPostParameters();
							String id = params.get("id");
							if (id == null) {
								return Promise.of(HttpResponse.ofCode(401));
							}
							pollDao.remove(Integer.parseInt(id));

							return Promise.of(HttpResponse.redirect302("/"));
						}));
		//[END REGION_4]
	}

	//[START REGION_5]
	public static void main(String[] args) throws Exception {
		Launcher launcher = new ApplicationLauncher();
		launcher.launch(args);
	}
	//[END REGION_5]
}
