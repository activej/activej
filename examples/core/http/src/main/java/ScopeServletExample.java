import io.activej.http.AsyncServlet;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.http.inject.RequestScope;
import io.activej.http.inject.ScopeServlet;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.promise.Promise;

import java.util.function.Function;

//[START EXAMPLE]
public final class ScopeServletExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet(Injector injector) {
		return new ScopeServlet(injector) {
			@Provides
			Function<Object[], String> template() {
				return args -> String.format("Hello world from ActiveInject Servlet%n%n%1$s", args);
			}

			@Provides
			@RequestScope
			String content(HttpRequest request, Function<Object[], String> template) {
				return template.apply(new Object[]{request});
			}

			@Provides
			@RequestScope
			Promise<HttpResponse> httpResponse(String content) {
				return Promise.of(HttpResponse.ok200().withPlainText(content));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		ScopeServletExample example = new ScopeServletExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
