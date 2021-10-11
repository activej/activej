/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.launcher;

import io.activej.inject.Injector;
import io.activej.inject.InstanceInjector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.jmx.api.ConcurrentJmxBeanAdapter;
import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.launcher.annotation.Args;
import io.activej.launcher.annotation.OnComplete;
import io.activej.launcher.annotation.OnRun;
import io.activej.launcher.annotation.OnStart;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static io.activej.inject.util.Utils.makeGraphVizGraph;
import static io.activej.types.Types.parameterizedType;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toSet;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Integrates all modules together and manages application lifecycle by
 * passing several steps:
 * <ul>
 * <li>wiring modules</li>
 * <li>starting services</li>
 * <li>running</li>
 * <li>stopping services</li>
 * </ul>
 * <p>
 * Example.<br>
 * Prerequisites: an application consists of three modules, which preferably
 * should be configured using separate configs and may depend on each other.
 * <pre>
 * public class ApplicationLauncher extends Launcher {
 *
 *    &#64;Override
 *    protected Collection&#60;Module&#62; getModules() {
 *        return null;
 *    }
 *
 *    &#64;Override
 *    protected void run() {
 *        System.out.println("Hello world");
 *    }
 *
 *    public static void main(String[] args) throws Exception {
 *        ApplicationLauncher launcher = new ApplicationLauncher();
 *        launcher.launch(true, args);
 *    }
 * }
 * </pre>
 */
@SuppressWarnings({"WeakerAccess", "RedundantThrows", "unused"})
@JmxBean(ConcurrentJmxBeanAdapter.class)
public abstract class Launcher {
	public static final Key<Set<InstanceInjector<?>>> INSTANCE_INJECTORS_KEY = new Key<Set<InstanceInjector<?>>>() {};

	protected final Logger logger = getLogger(getClass());
	protected final Logger logger0 = getLogger(getClass().getName() + ".0");

	public static final String[] NO_ARGS = {};
	protected String[] args = NO_ARGS;

	private Thread mainThread;

	private volatile Throwable applicationError;

	private volatile Instant instantOfLaunch;
	private volatile Instant instantOfStart;
	private volatile Instant instantOfRun;
	private volatile Instant instantOfStop;
	private volatile Instant instantOfComplete;

	private final CountDownLatch shutdownLatch = new CountDownLatch(1);
	private final CountDownLatch completeLatch = new CountDownLatch(1);

	private final CompletableFuture<Void> onStartFuture = new CompletableFuture<>();
	private final CompletableFuture<Void> onRunFuture = new CompletableFuture<>();
	private final CompletableFuture<Void> onCompleteFuture = new CompletableFuture<>();

	/**
	 * Creates an injector with modules and overrides from this launcher.
	 * On creation, it does all the binding checks so calling this method
	 * triggers a static check which causes an exception to be thrown on
	 * any incorrect bindings (unsatisfied or cyclic dependencies)
	 * which is highly useful for testing.
	 */
	public final void testInjector() {
		createInjector();
	}

	/**
	 * Launch application following few simple steps:
	 * <ul>
	 * <li>Inject dependencies</li>
	 * <li>Starts application, {@link Launcher#onStart()} is called in this stage</li>
	 * <li>Runs application, {@link Launcher#run()} is called in this stage</li>
	 * <li>Stops application, {@link Launcher#onStop()} is called in this stage</li>
	 * </ul>
	 * You can override methods mentioned above to execute your code in needed stage.
	 *
	 * @param args program args that will be injected into @Args string array
	 */
	public final void launch(String[] args) throws Exception {
		mainThread = Thread.currentThread();
		instantOfLaunch = Instant.now();

		try {
			logger.info("=== INJECTING DEPENDENCIES");

			Injector injector = createInjector(args);
			injector.getInstance(this.getClass());
			if (logger0.isInfoEnabled()) {
				logger0.info("Effective Injector:\n\n{}", makeGraphVizGraph(injector.getBindingsTrie()));
			}

			onInit(injector);

			injector.createEagerInstances();
			logger0.info("Created eager singletons");

			Set<LauncherService> services = injector.getInstanceOr(new Key<Set<LauncherService>>() {}, emptySet());
			Set<LauncherService> startedServices = new HashSet<>();

			logger0.info("Post-injected instances: {}", postInjectInstances(injector));

			logger.info("=== STARTING APPLICATION");
			try {
				instantOfStart = Instant.now();
				logger0.info("Starting Root Services: {}", services);
				startServices(services, startedServices);
				onStart();
				onStartFuture.complete(null);
			} catch (Exception e) {
				applicationError = e;
				logger.error("Start error", e);
				onStartFuture.completeExceptionally(e);
			}

			if (applicationError == null) {
				logger.info("=== RUNNING APPLICATION");
				try {
					instantOfRun = Instant.now();
					run();
					onRunFuture.complete(null);
				} catch (Exception e) {
					applicationError = e;
					logger.error("Error", e);
					onRunFuture.completeExceptionally(e);
				}
			} else {
				onRunFuture.completeExceptionally(applicationError);
			}

			logger.info("=== STOPPING APPLICATION");
			instantOfStop = Instant.now();
			if (!onStartFuture.isCompletedExceptionally()) {
				try {
					onStop();
				} catch (Exception e) {
					logger.error("Stop error", e);
				}
			}

			stopServices(startedServices);

			if (applicationError == null) {
				onCompleteFuture.complete(null);
			} else {
				onCompleteFuture.completeExceptionally(applicationError);
				throw applicationError;
			}

		} catch (Exception e) {
			throw e;
		} catch (Throwable e) {
			applicationError = e;
			logger.error("JVM Fatal Error", e);
			System.exit(-1);
		} finally {
			instantOfComplete = Instant.now();
			completeLatch.countDown();
		}
	}

	@SuppressWarnings("unchecked")
	private Set<Key<?>> postInjectInstances(Injector injector) {
		Set<InstanceInjector<?>> postInjectors = injector.getInstanceOr(INSTANCE_INJECTORS_KEY, emptySet());
		for (InstanceInjector<?> instanceInjector : postInjectors) {
			Object instance = injector.peekInstance(instanceInjector.key());
			if (instance != null) {
				((InstanceInjector<Object>) instanceInjector).injectInto(instance);
			}
		}
		return postInjectors.stream().map(InstanceInjector::key).collect(toSet());
	}

	private void startServices(Collection<LauncherService> services, Collection<LauncherService> startedServices) throws Throwable {
		List<Throwable> exceptions = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(services.size());
		synchronized (this) {
			for (LauncherService service : services) {
				if (!exceptions.isEmpty()) {
					latch.countDown();
					continue;
				}
				logger0.info("Starting Root Service: {}", service);
				service.start().whenComplete(($, e) -> {
					synchronized (this) {
						if (e == null) {
							startedServices.add(service);
						} else {
							exceptions.add(
									(e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null ? e.getCause() : e);
						}
						latch.countDown();
					}
				});
			}
		}
		latch.await();
		if (!exceptions.isEmpty()) {
			exceptions.sort(comparingInt(e -> (e instanceof RuntimeException) ? 1 : (e instanceof Error ? 0 : 2)));
			throw exceptions.get(0);
		}
	}

	private void stopServices(Collection<LauncherService> startedServices) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(startedServices.size());
		for (LauncherService service : startedServices) {
			logger0.info("Stopping Root Service: {}", service);
			service.stop().whenComplete(($, e) -> {
				if (e != null) {
					logger.error("Stop error in " + service,
							(e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null ? e.getCause() : e);
				}
				latch.countDown();
			});
		}
		latch.await();
	}

	public final @NotNull Injector createInjector(String[] args) {
		this.args = args;
		return createInjector();
	}

	public final @NotNull Injector createInjector() {
		return Injector.of(getInternalModule().combineWith(getModule()).overrideWith(getOverrideModule()));
	}

	@SuppressWarnings("unchecked")
	private Module getInternalModule() {
		Class<Launcher> launcherClass = (Class<Launcher>) getClass();
		Key<CompletionStage<Void>> completionStageKey = new Key<CompletionStage<Void>>() {};

		return ModuleBuilder.create()
				.bind(String[].class, Args.class).toInstance(args)

				.bind(Launcher.class).to(launcherClass)
				.bind(launcherClass).toInstance(this)

				.bindIntoSet(INSTANCE_INJECTORS_KEY.getTypeParameter(0), Key.ofType(parameterizedType(InstanceInjector.class, launcherClass)))

				.bind(completionStageKey.qualified(OnStart.class)).toInstance(onStartFuture)
				.bind(completionStageKey.qualified(OnRun.class)).toInstance(onRunFuture)
				.bind(completionStageKey.qualified(OnComplete.class)).toInstance(onCompleteFuture)

				.scan(Launcher.this)
				.build();
	}

	/**
	 * Supplies business logic module for application(ConfigModule, EventloopModule, etc...)
	 */
	protected Module getModule() {
		return Module.empty();
	}

	/**
	 * This module overrides definitions in internal module / business logic module
	 */
	protected Module getOverrideModule() {
		return Module.empty();
	}

	/**
	 * This method runs prior using injector and wiring the application
	 */
	protected void onInit(Injector injector) throws Exception {
	}

	/**
	 * This method runs when application is starting
	 */
	protected void onStart() throws Exception {
	}

	/**
	 * Launcher's main method.
	 */
	protected abstract void run() throws Exception;

	/**
	 * This method runs when application is stopping
	 */
	protected void onStop() throws Exception {
	}

	private final Thread shutdownHook = new Thread(() -> {
		try {
			// release anything blocked at `awaitShutdown` call
			shutdownLatch.countDown();
			// then wait for the `launch` call to finish
			completeLatch.await();
			// and wait a bit for things after the `launch` call, such as JUnit finishing or whatever
			Thread.sleep(10);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Shutdown thread got interrupted", e);
		}
	}, "shutdownNotification");

	/**
	 * Blocks the current thread until shutdown notification releases it.
	 * <br>
	 * Shutdown notification is released on JVM shutdown or by calling {@link Launcher#shutdown()}
	 */
	protected final void awaitShutdown() throws InterruptedException {
		// check if shutdown is not in process already
		if (shutdownLatch.getCount() != 1) {
			return;
		}
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		shutdownLatch.await();
	}

	/**
	 * Manually releases all threads waiting for shutdown.
	 *
	 * @see Launcher#awaitShutdown()
	 */
	public final void shutdown() {
		Runtime.getRuntime().removeShutdownHook(shutdownHook);
		shutdownLatch.countDown();
	}

	public final @NotNull Thread getMainThread() {
		return mainThread;
	}

	public final String[] getArgs() {
		return args;
	}

	public final CompletionStage<Void> getStartFuture() {
		return onStartFuture;
	}

	public final CompletionStage<Void> getRunFuture() {
		return onRunFuture;
	}

	public final CompletionStage<Void> getCompleteFuture() {
		return onCompleteFuture;
	}

	@JmxAttribute
	public final @Nullable Instant getInstantOfLaunch() {
		return instantOfLaunch;
	}

	@JmxAttribute
	public final @Nullable Instant getInstantOfStart() {
		return instantOfStart;
	}

	@JmxAttribute
	public final @Nullable Instant getInstantOfRun() {
		return instantOfRun;
	}

	@JmxAttribute
	public final @Nullable Instant getInstantOfStop() {
		return instantOfStop;
	}

	@JmxAttribute
	public final @Nullable Instant getInstantOfComplete() {
		return instantOfComplete;
	}

	@JmxAttribute
	public final @Nullable Duration getDurationOfStart() {
		if (instantOfLaunch == null) {
			return null;
		}
		return Duration.between(instantOfLaunch, instantOfRun == null ? Instant.now() : instantOfRun);
	}

	@JmxAttribute
	public final @Nullable Duration getDurationOfRun() {
		if (instantOfRun == null) {
			return null;
		}
		return Duration.between(instantOfRun, instantOfStop == null ? Instant.now() : instantOfStop);
	}

	@JmxAttribute
	public final @Nullable Duration getDurationOfStop() {
		if (instantOfStop == null) {
			return null;
		}
		return Duration.between(instantOfStop, instantOfComplete == null ? Instant.now() : instantOfComplete);
	}

	@JmxAttribute
	public final @Nullable Duration getDuration() {
		if (instantOfLaunch == null) {
			return null;
		}
		return Duration.between(instantOfLaunch, instantOfComplete == null ? Instant.now() : instantOfComplete);
	}

	@JmxAttribute
	public final @Nullable Throwable getApplicationError() {
		return applicationError;
	}
}
