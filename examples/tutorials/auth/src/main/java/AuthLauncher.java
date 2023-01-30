import io.activej.http.*;
import io.activej.http.loader.IStaticLoader;
import io.activej.http.session.ISessionStore;
import io.activej.http.session.InMemorySessionStore;
import io.activej.http.session.SessionServlet;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

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
	IStaticLoader staticLoader(Reactor reactor, Executor executor) {
		return IStaticLoader.ofClassPath(reactor, executor, "site/");
	}

	@Provides
	ISessionStore<String> sessionStore(Reactor reactor) {
		return InMemorySessionStore.<String>builder(reactor)
				.withLifetime(Duration.ofDays(30))
				.build();
	}

	@Provides
	AsyncServlet servlet(Reactor reactor, ISessionStore<String> sessionStore,
			@Named("public") AsyncServlet publicServlet, @Named("private") AsyncServlet privateServlet) {
		return SessionServlet.create(reactor, sessionStore, SESSION_ID, publicServlet, privateServlet);
	}
	//[END REGION_1]

	//[START REGION_2]
	@Provides
	@Named("public")
	AsyncServlet publicServlet(Reactor reactor, AuthService authService, ISessionStore<String> store, IStaticLoader staticLoader) {
		StaticServlet staticServlet = StaticServlet.create(reactor, staticLoader, "errorPage.html");
		return RoutingServlet.create(reactor)
				//[START REGION_3]
				.map("/", request -> HttpResponse.redirect302("/login"))
				//[END REGION_3]
				.map(GET, "/signup", StaticServlet.create(reactor, staticLoader, "signup.html"))
				.map(GET, "/login", StaticServlet.create(reactor, staticLoader, "login.html"))
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
	AsyncServlet privateServlet(Reactor reactor, IStaticLoader staticLoader) {
		return RoutingServlet.create(reactor)
				//[START REGION_6]
				.map("/", request -> HttpResponse.redirect302("/members"))
				//[END REGION_6]
				//[START REGION_7]
				.map("/members/*", RoutingServlet.create(reactor)
						.map(GET, "/", StaticServlet.create(reactor, staticLoader, "index.html"))
						//[START REGION_8]
						.map(GET, "/cookie", request ->
								HttpResponse.ok200().withBody(wrapUtf8(request.getAttachment(String.class))))
						//[END REGION_8]
						.map(POST, "/logout", request ->
								HttpResponse.redirect302("/")
										.withCookie(HttpCookie.builder(SESSION_ID)
												.withPath("/")
												.withMaxAge(Duration.ZERO)
												.build())));
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
