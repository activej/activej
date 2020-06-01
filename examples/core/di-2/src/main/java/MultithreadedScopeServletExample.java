import io.activej.di.Injector;
import io.activej.di.annotation.Named;
import io.activej.di.annotation.Provides;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.http.di.RequestScope;
import io.activej.http.di.ScopeServlet;
import io.activej.launchers.http.MultithreadedHttpServerLauncher;
import io.activej.promise.Promise;
import io.activej.worker.Worker;
import io.activej.worker.WorkerId;

import java.util.function.Function;

//[START EXAMPLE]
public final class MultithreadedScopeServletExample extends MultithreadedHttpServerLauncher {
	@Provides
	String string() {
		return "root string";
	}

	@Provides
	@Worker
	AsyncServlet servlet(@Named("1") AsyncServlet servlet1, @Named("2") AsyncServlet servlet2) {
		return RoutingServlet.create()
				.map("/", request -> HttpResponse.ok200()
						.withHtml("<a href=\"/first\">first</a><br><a href=\"/second\">second</a>"))
				.map("/first", servlet1)
				.map("/second", servlet2);
	}

	@Provides
	@Worker
	@Named("1")
	AsyncServlet servlet1(Injector injector) {
		return new ScopeServlet(injector) {
			@Provides
			Function<Object[], String> template(String rootString) {
				return args -> String.format(rootString + "\nHello1 from worker server %1$s\n\n%2$s", args);
			}

			@Provides
			@RequestScope
			String content(HttpRequest request, @WorkerId int workerId, Function<Object[], String> template) {
				return template.apply(new Object[]{workerId, request});
			}

			//[START REGION_1]
			@Provides
			@RequestScope
			Promise<HttpResponse> httpResponse(String content) {
				return Promise.of(HttpResponse.ok200().withPlainText(content));
			}
			//[END REGION_1]
		};
	}

	@Provides
	@Worker
	@Named("2")
	AsyncServlet servlet2(Injector injector) {
		return new ScopeServlet(injector) {
			@Provides
			Function<Object[], String> template(String rootString) {
				return args -> String.format(rootString + "\nHello2 from worker server %1$s\n\n%2$s", args);
			}

			@Provides
			@RequestScope
			String content(HttpRequest request, @WorkerId int workerId, Function<Object[], String> template) {
				return template.apply(new Object[]{workerId, request});
			}

			//[START REGION_2]
			@Provides
			@RequestScope
			HttpResponse httpResponse(String content) {
				return HttpResponse.ok200().withPlainText(content);
			}
			//[END REGION_2]
		};
	}

	public static void main(String[] args) throws Exception {
		MultithreadedScopeServletExample example = new MultithreadedScopeServletExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
