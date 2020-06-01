import io.activej.di.Injector;
import io.activej.di.module.AbstractModule;

/**
 * A module is a collection of bindings and a binding is a "recipe" of how to make an instance of certain type.
 * You simply define all these "recipes" with DSL.
 * <p>
 * - In this example you make a lambda which creates an instance of ConsoleMessageSenderImpl which is a recipe for it.
 * - Then you say that to create an instance of MessageSender you can use the ConsoleMessageSenderImpl, which is already known how to make.
 * - And then you add a recipe with a lambda for an Application instance, which depends on MessageSender, which is known to be made as a ConsoleMessageSenderImpl.
 * <p>
 * Keep in mind, that all of those "recipes" can be split into multiple modules, and you don't need to know an exact implementation of the MessageSender
 * interface, you just know that somewhere somebody provided a recipe for it. That is the whole point of the Dependency Injection paradigm.
 * <p>
 * One notable difference is that all the recipes are by default 'singletons' - meaning that
 * an object is created from the recipe at most once per its key per injector instance, cached and reused for any other recipes that depend on it.
 */
//[START EXAMPLE]
public final class CoreExample {

	interface MessageSender {

		void send(String message);
	}

	static class ConsoleMessageSenderImpl implements MessageSender {

		@Override
		public void send(String message) {
			System.out.println("received message: " + message);
		}
	}

	static class Application {
		private final MessageSender sender;

		Application(MessageSender sender) {
			this.sender = sender;
		}

		void hello() {
			sender.send("hello from application");
		}
	}

	static class SomeModuleWithImpls extends AbstractModule {

		@Override
		protected void configure() {
			// this module knows how to make an instance of ConsoleMessageSenderImpl
			// no reflection yet
			bind(ConsoleMessageSenderImpl.class).to(ConsoleMessageSenderImpl::new);
		}
	}

	static class ApplicationModule extends AbstractModule {
		@Override
		protected void configure() {

			// we *bind* the interface to its implementation, just like any other DI
			bind(MessageSender.class).to(ConsoleMessageSenderImpl.class);

			// and also teach it how to make an application instance
			bind(Application.class).to(Application::new, MessageSender.class);
		}
	}

	public static void main(String[] args) {
		Injector injector = Injector.of(new SomeModuleWithImpls(), new ApplicationModule());
		Application application = injector.getInstance(Application.class);

		application.hello();
	}
}
//[END EXAMPLE]
