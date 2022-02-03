import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ScopeAnnotation;
import io.activej.inject.module.AbstractModule;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Scopes in ActiveJ Inject are a bit different from other DI frameworks.
 * The internal structure of the injector is not simply a map of Key -> Binding.
 * It is actually a <a href="https://en.wikipedia.org/wiki/Trie">prefix tree</a> of such maps, and the prefix is a scope.
 * The identifiers (or prefixes of the tree) just like Names are annotations, this is highly useful for the DSL.
 * <p>
 * Bindings can use either local to them (in the same map) keys as dependencies or the ones that are closer to the root
 * (the "upper" ones if tree is drawn as growing from top to bottom).
 * <p>
 * Any injector has a link to the whole tree, which is statically built from modules, checked for missing or cyclic dependencies,
 * and so called `current scope` - the scope (represented as an array of prefixes from the root of the tree) at which given injector operates.
 * Default injector operates at the root, the array is of length zero.
 * An injector can `enter the scope`, it means to create an injector with the scope set to the one that you are entering.
 * This can be done multiple times, so you can have N injectors in certain scope.
 * An injector creates a new instance if the requested key can be found in its local bindings, and if not, it delegates the instance creation to the parent injector.
 * So for example, when you enter scope TestScope N times, you have N injector and an ability to create N instances of any keys that are in that scope, yet
 * if they depend on some key from the upper scope then it is created/reused by the parent injector.
 * <p>
 * So if you have Scope1 and Scope2, you can enter Scope1 N times and from each of those injectors you can enter Scope2 M times and in total
 * have N*M instances of keys that are in scope Scope1->Scope2, N instances of each key from Scope1, M instances of each key from Scope2,
 * and 1 instance of each key from the root scope.
 * <p>
 * In the example below, we will create a HttpScope, which will have a binding that knows how to make an HttpResponse instance from given HttpRequest instance.
 * It also depends on some key from HttpScope as well as on some key from the root scope.
 * As you can see in the code, the only non-intuitive part is how to pass different HttpRequests in each created sub injector.
 * The binding prefix tree is compiled and checked at the startup time, so we cannot just override the binding when entering the scope.
 * Instead, we override the instance.
 * An injector has a mapping of instances that it already created, so that it could return the one that was already made instead of making it again.
 * And into that mapping we are pre-making the instance, so the injector would return it when requested.
 * The only left thing is the static dependency check, so we have to have a dummy binding that will never be actually called, but that will satisfy
 * the checker, and so we add one.
 * <p>
 * No additional configurations or registrations are needed, we just added a binding deeper into the prefix tree of bindings and used that deeper tree leaf
 * when making a set of sub-injectors each of which created its own "singleton" instance we requested.
 * <p>
 * Scopes are a very powerful tool for managing numbers of instances that you create (since one Injector creates at most one instance per key)
 * yet still maintaining the simplicity and speed of the ActiveJ Inject framework.
 */
//[START EXAMPLE]
public final class ScopeExample {

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@ScopeAnnotation
	@interface HttpScope {
	}

	record HttpRequest(String ping) {}

	record HttpResponse(String pong) {}

	static class HttpModule extends AbstractModule {

		@Override
		protected void configure() {
			// when your factory returns null it means that it refuses to construct an instance
			// and so it throws a nice CannotConstructException if called
			bind(HttpRequest.class).in(HttpScope.class).to(() -> {throw new AssertionError();});
		}
	}

	static class ApplicationModule extends AbstractModule {
		@Override
		protected void configure() {
			install(new HttpModule());
		}

		@Provides
		String topLevel() {
			System.err.println("top-level dependency created");
			return "hello";
		}

		@HttpScope
		@Provides
		Integer scopeLevel() {
			System.err.println("scope-level dependency created");
			return 42;
		}

		@HttpScope
		@Provides
		HttpResponse process(HttpRequest httpRequest, String topLevel, Integer scopeLevel) {
			return new HttpResponse("was on server: " + httpRequest.ping);
		}
	}

	static final Scope HTTP_SCOPE = Scope.of(HttpScope.class);

	public static void main(String[] args) {
		Injector injector = Injector.of(new ApplicationModule());

		for (int i = 0; i < 10; i++) {
			Injector subInjector = injector.enterScope(HTTP_SCOPE);
			subInjector.putInstance(Key.of(HttpRequest.class), new HttpRequest("ping: " + i));
			System.out.println(subInjector.getInstance(HttpResponse.class).pong);
		}

		try {
			injector.enterScope(HTTP_SCOPE).enterScope(HTTP_SCOPE);
		} catch (Exception e) {
			System.err.println("\nNo bindings in scope ()->@HttpScope()->@HttpScope()\n");
		}
	}
}
//[END EXAMPLE]
