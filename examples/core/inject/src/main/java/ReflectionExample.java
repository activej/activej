import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.module.AbstractModule;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.logging.Logger;

/**
 * - First binding is a simple binding where we even used a shortcut and didn't use a lambda since we already made an instance.
 * - Next, we bind a message sender to its implementation. Note the .annotatedWith DSL call - we specify what key exactly we are binding.
 * Note that we did not describe how to create ConsoleMessageSenderImpl instance - it has {@link Inject} annotation on it which means that
 * a binding that will actually make the instance will be generated automatically and call its default constructor via reflection.
 * - Same as above, but with another syntax and a custom annotation - we make a concrete Key instance and bind it to the implementation.
 * The implementation is using an inject constructor - constructor from which a binding will be made automatically
 * - And lastly, we use a simple bind call without ending for the annotation class - requesting a binding to be made in same way as above.
 * The generated binding will call the selected constructor (and via inject annotation on the class we've chosen the default constructor)
 * as well as set the fields marked with Inject annotation to instances made by keys from their type and annotation.
 */
//[START EXAMPLE]
public final class ReflectionExample {

	interface MessageSender {

		void send(String message);
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	@QualifierAnnotation
	@interface SecondKey {
	}

	@Inject
	static class ConsoleMessageSenderImpl implements MessageSender {
		@Override
		public void send(String message) {
			System.out.println("received message: " + message);
		}
	}

	static class LoggingMessageSenderImpl implements MessageSender {
		private final Logger logger;

		@Inject
		public LoggingMessageSenderImpl(Logger logger) {
			this.logger = logger;
		}

		@Override
		public void send(String message) {
			logger.info("received message: " + message);
		}
	}

	@Inject
	static class Application {

		@Inject
		@Named("first")
		private MessageSender sender;

		@Inject
		@SecondKey
		private MessageSender logger;

		void hello() {
			sender.send("hello from application");
			logger.send("logged greeting");
		}
	}

	static class ApplicationModule extends AbstractModule {
		@Override
		protected void configure() {
			bind(Logger.class).toInstance(Logger.getLogger("example"));

			// it knows how to make instances of impls because they have @Inject annotations
			// allowing us to know how they can be created
			// we never automagically create instances of unaware classes
			bind(MessageSender.class, "first").to(ConsoleMessageSenderImpl.class);
			bind(MessageSender.class, SecondKey.class).to(LoggingMessageSenderImpl.class);

			// same as above, just trigger the automatic factory generation from the marked constructor
			bind(Application.class);
		}
	}

	public static void main(String[] args) {
		Injector injector = Injector.of(new ApplicationModule());
		Application application = injector.getInstance(Application.class);

		application.hello();
	}
}
//[END EXAMPLE]
