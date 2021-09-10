import io.activej.http.*;
import io.activej.http.loader.StaticLoader;
import io.activej.http.session.SessionServlet;
import io.activej.http.session.SessionStore;
import io.activej.http.session.SessionStoreInMemory;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START REGION_1]
public final class AuthLauncher extends HttpServerLauncher {
	public static final String SESSION_ID = "SESSION_ID";

	@Provides
	AuthService loginService() {
		return new AuthServiceImpl();
	}

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	@Provides
	StaticLoader staticLoader(Executor executor) {
		return StaticLoader.ofClassPath(executor, "site/");
	}

	@Provides
	SessionStore<String> sessionStore() {
		return SessionStoreInMemory.<String>create()
				.withLifetime(Duration.ofDays(30));
	}

	@Provides
	AsyncServlet servlet(SessionStore<String> sessionStore,
			@Named("public") AsyncServlet publicServlet, @Named("private") AsyncServlet privateServlet) {
		return SessionServlet.create(sessionStore, SESSION_ID, publicServlet, privateServlet);
	}
	//[END REGION_1]

	//[START REGION_2]
	@Provides
	@Named("public")
	AsyncServlet publicServlet(AuthService authService, SessionStore<String> store, StaticLoader staticLoader) {
		StaticServlet staticServlet = StaticServlet.create(staticLoader, "errorPage.html");
		return RoutingServlet.create()
				//[START REGION_3]
				.map("/", request -> HttpResponse.redirect302("/login"))
				//[END REGION_3]
				.map(GET, "/signup", StaticServlet.create(staticLoader, "signup.html"))
				.map(GET, "/login", StaticServlet.create(staticLoader, "login.html"))
				//[START REGION_4]
				.map(POST, "/login", request -> request.loadBody()
						.then(() -> {
							Map<String, String> params = request.getPostParameters();
							String username = params.get("username");
							String password = params.get("password");
							if (authService.authorize(username, password)) {
								String sessionId = UUID.randomUUID().toString();

								return store.save(sessionId, "My object saved in session")
										.map($ -> HttpResponse.redirect302("/members")
												.withCookie(HttpCookie.of(SESSION_ID, sessionId)));
							}
							return staticServlet.serve(request);
						}))
				//[END REGION_4]
				.map(POST, "/signup", request -> request.loadBody()
						.map($ -> {
							Map<String, String> params = request.getPostParameters();
							String username = params.get("username");
							String password = params.get("password");

							if (username != null && password != null) {
								authService.register(username, password);
							}
							return HttpResponse.redirect302("/login");
						}));
	}
	//[END REGION_2]

	//[START REGION_5]
	@Provides
	@Named("private")
	AsyncServlet privateServlet(StaticLoader staticLoader) {
		return RoutingServlet.create()
				//[START REGION_6]
				.map("/", request -> HttpResponse.redirect302("/members"))
				//[END REGION_6]
				//[START REGION_7]
				.map("/members/*", RoutingServlet.create()
						.map(GET, "/", StaticServlet.create(staticLoader, "index.html"))
						//[START REGION_8]
						.map(GET, "/cookie", request ->
								HttpResponse.ok200().withBody(wrapUtf8(request.getAttachment(String.class))))
						//[END REGION_8]
						.map(POST, "/logout", request ->
								HttpResponse.redirect302("/")
										.withCookie(HttpCookie.of(SESSION_ID).withPath("/").withMaxAge(Duration.ZERO))));
		//[END REGION_7]
	}
	//[END REGION_5]

	//[START REGION_9]
	public static void main(String[] args) throws Exception {
		AuthLauncher launcher = new AuthLauncher();
		launcher.launch(args);
	}
	//[END REGION_9]
}
