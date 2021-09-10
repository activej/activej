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

package io.activej.service;

import io.activej.async.service.EventloopService;
import io.activej.common.initializer.Initializer;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.service.BlockingService;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.BlockingSocketServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.annotation.Optional;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.Dependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.util.ScopedValue;
import io.activej.inject.util.Trie;
import io.activej.launcher.LauncherService;
import io.activej.net.EventloopServer;
import io.activej.service.adapter.ServiceAdapter;
import io.activej.service.adapter.ServiceAdapters;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.Closeable;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.difference;
import static io.activej.common.Utils.intersection;
import static io.activej.common.reflection.ReflectionUtils.isClassPresent;
import static io.activej.inject.binding.BindingType.TRANSIENT;
import static io.activej.service.Utils.combineAll;
import static io.activej.service.Utils.completedExceptionallyFuture;
import static io.activej.service.adapter.ServiceAdapters.*;
import static java.lang.Thread.currentThread;
import static java.util.Collections.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Builds dependency graph of {@code Service} objects based on DI's object
 * graph. Service graph module is capable to start services concurrently.
 * <p>
 * Consider some lifecycle details of this module:
 * <ul>
 * <li>
 * Put all objects from the graph which can be treated as
 * {@link Service} instances.
 * </li>
 * <li>
 * Starts services concurrently starting at leaf graph nodes (independent
 * services) and ending with root nodes.
 * </li>
 * <li>
 * Stop services starting from root and ending with independent services.
 * </li>
 * </ul>
 * <p>
 * An ability to use {@link ServiceAdapter} objects allows to create a service
 * from any object by providing it's {@link ServiceAdapter} and registering
 * it in {@code ServiceGraphModule}. Take a look at {@link ServiceAdapters},
 * which has a lot of implemented adapters. Its necessarily to annotate your
 * object provider with {@link Worker @Worker} or Singleton
 * annotation.
 * <p>
 * An application terminates if a circular dependency found.
 */
public final class ServiceGraphModule extends AbstractModule implements ServiceGraphModuleSettings, WithInitializer<ServiceGraphModule> {
	private static final Logger logger = getLogger(ServiceGraphModule.class);

	private final Map<Class<?>, ServiceAdapter<?>> registeredServiceAdapters = new LinkedHashMap<>();
	private final Set<Key<?>> excludedKeys = new LinkedHashSet<>();
	private final Map<Key<?>, ServiceAdapter<?>> keys = new LinkedHashMap<>();

	private final Map<Key<?>, Set<Key<?>>> addedDependencies = new HashMap<>();
	private final Map<Key<?>, Set<Key<?>>> removedDependencies = new HashMap<>();

	private final Executor executor;

	public ServiceGraphModule() {
		this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				10, TimeUnit.MILLISECONDS,
				new SynchronousQueue<>());
	}

	/**
	 * Creates a service graph with default configuration, which is able to
	 * handle {@code Service, BlockingService, Closeable, ExecutorService,
	 * Timer, DataSource, EventloopService, EventloopServer} and
	 * {@code Eventloop} as services.
	 *
	 * @return default service graph
	 */
	public static ServiceGraphModule create() {
		ServiceGraphModule serviceGraphModule = new ServiceGraphModule()
				.register(Service.class, forService())
				.register(BlockingService.class, forBlockingService())
				.register(Closeable.class, forCloseable())
				.register(ExecutorService.class, forExecutorService())
				.register(Timer.class, forTimer())
				.withInitializer(module -> {
					try {
						currentThread().getContextClassLoader().loadClass("javax.sql.DataSource");
						module.register(DataSource.class, forDataSource());
					} catch (ClassNotFoundException ignored) {
					}
				});

		tryRegisterAsyncComponents(serviceGraphModule);

		return serviceGraphModule;
	}

	/**
	 * Puts an instance of class and its factory to the factoryMap
	 *
	 * @param <T>     type of service
	 * @param type    key with which the specified factory is to be associated
	 * @param factory value to be associated with the specified type
	 * @return ServiceGraphModule with change
	 */
	@Override
	public <T> ServiceGraphModule register(Class<? extends T> type, ServiceAdapter<T> factory) {
		registeredServiceAdapters.put(type, factory);
		return this;
	}

	/**
	 * Puts the key and its factory to the keys
	 *
	 * @param key     key with which the specified factory is to be associated
	 * @param factory value to be associated with the specified key
	 * @param <T>     type of service
	 * @return ServiceGraphModule with change
	 */
	@Override
	public <T> ServiceGraphModule registerForSpecificKey(Key<T> key, ServiceAdapter<T> factory) {
		keys.put(key, factory);
		return this;
	}

	@Override
	public <T> ServiceGraphModule excludeSpecificKey(Key<T> key) {
		excludedKeys.add(key);
		return this;
	}

	/**
	 * Adds the dependency for key
	 *
	 * @param key           key for adding dependency
	 * @param keyDependency key of dependency
	 * @return ServiceGraphModule with change
	 */
	@Override
	public ServiceGraphModule addDependency(Key<?> key, Key<?> keyDependency) {
		addedDependencies.computeIfAbsent(key, key1 -> new HashSet<>()).add(keyDependency);
		return this;
	}

	/**
	 * Removes the dependency
	 *
	 * @param key           key for removing dependency
	 * @param keyDependency key of dependency
	 * @return ServiceGraphModule with change
	 */
	@Override
	public ServiceGraphModule removeDependency(Key<?> key, Key<?> keyDependency) {
		removedDependencies.computeIfAbsent(key, key1 -> new HashSet<>()).add(keyDependency);
		return this;
	}

	private static final class ServiceKey implements ServiceGraph.Key {
		private final @NotNull Key<?> key;
		private final @Nullable WorkerPool workerPool;

		private ServiceKey(@NotNull Key<?> key) {
			this.key = key;
			this.workerPool = null;
		}

		private ServiceKey(@NotNull Key<?> key, @NotNull WorkerPool workerPool) {
			this.key = key;
			this.workerPool = workerPool;
		}

		public @NotNull Key<?> getKey() {
			return key;
		}

		@Override
		public @NotNull Type getType() {
			return key.getType();
		}

		@Override
		public @Nullable String getSuffix() {
			return workerPool == null ? null : "" + workerPool.getSize();
		}

		@Override
		public @Nullable String getIndex() {
			return workerPool == null || workerPool.getId() == 0 ? null : "" + (workerPool.getId() + 1);
		}

		@Override
		public @Nullable Object getQualifier() {
			return key.getQualifier();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ServiceKey other = (ServiceKey) o;
			return workerPool == other.workerPool &&
					key.equals(other.key);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, workerPool);
		}

		@Override
		public String toString() {
			return key + (workerPool == null ? "" : ":" + workerPool.getId());
		}
	}

	//  Registers service adapters for asynchronous components if they are present in classpath
	private static void tryRegisterAsyncComponents(ServiceGraphModule serviceGraphModule) {
		if (isClassPresent("io.activej.eventloop.Eventloop")) {
			// 'eventloop' module is present
			serviceGraphModule
					.register(Eventloop.class, forEventloop())
					.register(EventloopService.class, forEventloopService());
		}
		if (isClassPresent("io.activej.net.EventloopServer")) {
			// 'net' module is present
			serviceGraphModule
					.register(BlockingSocketServer.class, forBlockingSocketServer())
					.register(EventloopServer.class, forEventloopServer());
		}
	}

	@Provides
	ServiceGraph serviceGraph(Injector injector) {
		ServiceGraph serviceGraph = ServiceGraph.create();
		serviceGraph.setStartCallback(() -> doStart(serviceGraph, injector));
		return serviceGraph;
	}

	@ProvidesIntoSet
	LauncherService service(Injector injector, ServiceGraph serviceGraph, @Optional Set<Initializer<ServiceGraphModuleSettings>> initializers) {
		if (initializers != null) {
			for (Initializer<ServiceGraphModuleSettings> initializer : initializers) {
				initializer.accept(this);
			}
		}
		return new LauncherService() {
			@Override
			public CompletableFuture<?> start() {
				CompletableFuture<Void> future = new CompletableFuture<>();
				serviceGraph.startFuture()
						.whenComplete(($, e) -> {
							if (e == null) {
								if (logger.isInfoEnabled()) {
									logger.info("Effective ServiceGraph:\n\n{}", serviceGraph);
								}
								future.complete(null);
							} else {
								logger.error("Could not start ServiceGraph", e);
								if (logger.isInfoEnabled()) {
									logger.info("Effective ServiceGraph:\n\n{}", serviceGraph);
								}
								logger.warn("Stopping services of partially started ServiceGraph...");
								serviceGraph.stopFuture()
										.whenComplete(($2, e2) -> {
											if (e2 != null) {
												e.addSuppressed(e2);
											}
											future.completeExceptionally(e);
										});
							}
						});
				return future;
			}

			@Override
			public CompletableFuture<?> stop() {
				logger.info("Stopping ServiceGraph...");
				return serviceGraph.stopFuture();
			}
		};
	}

	private void doStart(ServiceGraph serviceGraph, Injector injector) {
		logger.trace("Initializing ServiceGraph ...");

		WorkerPools workerPools = injector.peekInstance(WorkerPools.class);
		List<WorkerPool> pools = workerPools != null ? workerPools.getWorkerPools() : emptyList();
		Map<ServiceKey, List<?>> instances = new HashMap<>();
		Map<ServiceKey, Set<ServiceKey>> instanceDependencies = new HashMap<>();

		IdentityHashMap<Object, ServiceKey> workerInstanceToKey = new IdentityHashMap<>();
		if (workerPools != null) {
			for (WorkerPool pool : pools) {
				Map<Key<?>, Set<ScopedValue<Dependency>>> scopeDependencies = getScopeDependencies(injector, pool.getScope());
				for (Map.Entry<Key<?>, WorkerPool.Instances<?>> entry : pool.peekInstances().entrySet()) {
					Key<?> key = entry.getKey();
					WorkerPool.Instances<?> workerInstances = entry.getValue();
					if (!scopeDependencies.containsKey(key)) {
						continue;
					}
					ServiceKey serviceKey = new ServiceKey(key, pool);
					instances.put(serviceKey, workerInstances.getList());
					workerInstanceToKey.put(workerInstances.get(0), serviceKey);
					instanceDependencies.put(serviceKey,
							scopeDependencies.get(key)
									.stream()
									.filter(scopedDependency -> {
										if (scopedDependency.get().isRequired()) {
											return true;
										}
										Injector container = scopedDependency.isScoped() ? pool.getScopeInjectors()[0] : injector;
										Key<?> k = scopedDependency.get().getKey();
										return container.hasInstance(k);
									})
									.map(scopedDependency -> scopedDependency.isScoped() ?
											new ServiceKey(scopedDependency.get().getKey(), pool) :
											new ServiceKey(scopedDependency.get().getKey()))
									.collect(toSet()));
				}
			}
		}

		for (Map.Entry<Key<?>, Object> entry : injector.peekInstances().entrySet()) {
			Key<?> key = entry.getKey();
			Object instance = entry.getValue();
			if (instance == null) continue;

			Binding<?> binding = injector.getBinding(key);

			if (binding == null || binding.getType() == TRANSIENT) continue;

			ServiceKey serviceKey = new ServiceKey(key);
			instances.put(serviceKey, singletonList(instance));
			instanceDependencies.put(serviceKey,
					binding.getDependencies().stream()
							.filter(dependency -> dependency.isRequired() || injector.hasInstance(dependency.getKey()))
							.map(dependency -> {
								Class<?> dependencyRawType = dependency.getKey().getRawType();
								boolean rawTypeMatches = dependencyRawType == WorkerPool.class || dependencyRawType == WorkerPools.class;
								boolean instanceMatches = instance instanceof WorkerPool.Instances;

								if (rawTypeMatches && instanceMatches) {
									WorkerPool.Instances<?> workerInstances = (WorkerPool.Instances<?>) instance;
									return workerInstanceToKey.get(workerInstances.get(0));
								}

								if (rawTypeMatches && !(instance instanceof WorkerPool)) {
									logger.warn("Unsupported service {} at {} : worker instances is expected", key, binding.getLocation());
								}

								if (instanceMatches) {
									logger.warn("Unsupported service {} at {} : dependency to WorkerPool or WorkerPools is expected", key, binding.getLocation());
								}
								return new ServiceKey(dependency.getKey());
							})
							.collect(toSet()));
		}

		doStart(serviceGraph, instances, instanceDependencies);
	}

	private Map<Key<?>, Set<ScopedValue<Dependency>>> getScopeDependencies(Injector injector, Scope scope) {
		Trie<Scope, Map<Key<?>, Binding<?>>> scopeBindings = injector.getBindingsTrie().getOrDefault(scope, emptyMap());
		return scopeBindings.get()
				.entrySet()
				.stream()
				.collect(toMap(Map.Entry::getKey,
						entry -> entry.getValue().getDependencies().stream()
								.map(dependencyKey ->
										scopeBindings.get().containsKey(dependencyKey.getKey()) ?
												ScopedValue.of(scope, dependencyKey) :
												ScopedValue.of(dependencyKey))
								.collect(toSet())));
	}

	private void doStart(ServiceGraph serviceGraph, Map<ServiceKey, List<?>> instances, Map<ServiceKey, Set<ServiceKey>> instanceDependencies) {
		IdentityHashMap<Object, CachedService> cache = new IdentityHashMap<>();

		Set<Key<?>> unusedKeys = difference(keys.keySet(), instances.keySet().stream().map(ServiceKey::getKey).collect(toSet()));
		if (!unusedKeys.isEmpty()) {
			logger.warn("Unused services : {}", unusedKeys);
		}

		for (Map.Entry<ServiceKey, List<?>> entry : instances.entrySet()) {
			ServiceKey serviceKey = entry.getKey();
			@Nullable Service service = getCombinedServiceOrNull(cache, serviceKey, entry.getValue());
			if (service != null) {
				serviceGraph.add(serviceKey, service);
			}
		}

		for (Map.Entry<ServiceKey, Set<ServiceKey>> entry : instanceDependencies.entrySet()) {
			ServiceKey serviceKey = entry.getKey();
			Key<?> key = serviceKey.getKey();
			Set<ServiceKey> dependencies = new HashSet<>(entry.getValue());

			if (!difference(removedDependencies.getOrDefault(key, emptySet()), dependencies).isEmpty()) {
				logger.warn("Unused removed dependencies for {} : {}", key, difference(removedDependencies.getOrDefault(key, emptySet()), dependencies));
			}

			if (!intersection(dependencies, addedDependencies.getOrDefault(key, emptySet())).isEmpty()) {
				logger.warn("Unused added dependencies for {} : {}", key, intersection(dependencies, addedDependencies.getOrDefault(key, emptySet())));
			}

			Set<Key<?>> added = addedDependencies.getOrDefault(key, emptySet());
			for (Key<?> k : added) {
				List<ServiceKey> found = instances.keySet().stream().filter(s -> s.getKey().equals(k)).collect(toList());
				if (found.isEmpty()) {
					throw new IllegalArgumentException("Did not find an instance for the added dependency " + key.getDisplayString());
				}
				if (found.size() > 1) {
					throw new IllegalArgumentException("Found more than one instance for the added dependency " + key.getDisplayString());
				}
				dependencies.add(found.get(0));
			}

			Set<Key<?>> removed = removedDependencies.getOrDefault(key, emptySet());
			dependencies.removeIf(k -> removed.contains(k.getKey()));

			for (ServiceKey dependency : dependencies) {
				serviceGraph.add(serviceKey, dependency);
			}
		}

		serviceGraph.removeIntermediateNodes();
	}

	@SuppressWarnings("unchecked")
	private @Nullable Service getCombinedServiceOrNull(IdentityHashMap<Object, CachedService> cache, ServiceKey key, List<?> instances) {
		List<Service> services = new ArrayList<>();
		for (Object instance : instances) {
			Service service = getServiceOrNull(cache, (Key<Object>) key.getKey(), instance);
			if (service != null) {
				services.add(service);
			}
		}
		if (services.isEmpty()) return null;
		return new CombinedService(services);
	}

	private static class CombinedService implements Service {
		private final List<Service> services;
		private final List<Service> startedServices = new ArrayList<>();

		private CombinedService(List<Service> services) {
			this.services = services;
		}

		@Override
		public CompletableFuture<?> start() {
			return combineAll(
					services.stream()
							.map(service -> safeCall(service::start)
									.thenRun(() -> {
										synchronized (this) {
											startedServices.add(service);
										}
									}))
							.collect(toList()))
					.thenApply(v -> (Throwable) null)
					.exceptionally(e -> e)
					.thenCompose((Throwable e) ->
							e == null ?
									completedFuture(null) :
									combineAll(startedServices.stream().map(service -> safeCall(service::stop)).collect(toList()))
											.thenCompose($ -> completedExceptionallyFuture(e)));
		}

		@Override
		public CompletableFuture<?> stop() {
			return combineAll(services.stream().map(service -> safeCall(service::stop)).collect(toList()));
		}

		private static <T> CompletionStage<T> safeCall(Supplier<? extends CompletionStage<T>> invoke) {
			try {
				return invoke.get();
			} catch (Exception e) {
				return completedExceptionallyFuture(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T> @Nullable Service getServiceOrNull(IdentityHashMap<Object, CachedService> cache, Key<T> key, @NotNull T instance) {
		CachedService service = cache.get(instance);
		if (service != null) {
			return service;
		}
		if (excludedKeys.contains(key)) {
			return null;
		}
		ServiceAdapter<T> serviceAdapter = lookupAdapter(key, (Class<T>) instance.getClass());
		if (serviceAdapter != null) {
			service = new CachedService(new Service() {
				@Override
				public CompletableFuture<?> start() {
					return serviceAdapter.start(instance, executor);
				}

				@Override
				public CompletableFuture<?> stop() {
					return serviceAdapter.stop(instance, executor);
				}
			});
			cache.put(instance, service);
			return service;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private <T> @Nullable ServiceAdapter<T> lookupAdapter(Key<T> key, Class<T> instanceClass) {
		ServiceAdapter<T> serviceAdapter = (ServiceAdapter<T>) keys.get(key);
		if (serviceAdapter == null) {
			List<Class<?>> foundRegisteredClasses = new ArrayList<>();
			l1:
			for (Map.Entry<Class<?>, ServiceAdapter<?>> entry : registeredServiceAdapters.entrySet()) {
				Class<?> registeredClass = entry.getKey();
				if (registeredClass.isAssignableFrom(instanceClass)) {
					Iterator<Class<?>> iterator = foundRegisteredClasses.iterator();
					while (iterator.hasNext()) {
						Class<?> foundRegisteredClass = iterator.next();
						if (registeredClass.isAssignableFrom(foundRegisteredClass)) {
							continue l1;
						}
						if (foundRegisteredClass.isAssignableFrom(registeredClass)) {
							iterator.remove();
						}
					}
					foundRegisteredClasses.add(registeredClass);
				}
			}

			if (foundRegisteredClasses.size() == 1) {
				serviceAdapter = (ServiceAdapter<T>) registeredServiceAdapters.get(foundRegisteredClasses.get(0));
			}
			if (foundRegisteredClasses.size() > 1) {
				throw new IllegalArgumentException("Ambiguous services found for " + instanceClass +
						" : " + foundRegisteredClasses + ". Use register() methods to specify service.");
			}
		}
		return serviceAdapter;
	}

	private static class CachedService implements Service {
		private final Service service;
		private int started;
		private CompletableFuture<?> startFuture;
		private CompletableFuture<?> stopFuture;

		private CachedService(Service service) {
			this.service = service;
		}

		@Override
		public synchronized CompletableFuture<?> start() {
			checkState(stopFuture == null, "Already stopped");
			started++;
			if (startFuture == null) {
				startFuture = service.start();
			}
			return startFuture;
		}

		@Override
		public synchronized CompletableFuture<?> stop() {
			checkState(startFuture != null, "Has not been started yet");
			if (--started != 0) return completedFuture(null);
			if (stopFuture == null) {
				stopFuture = service.stop();
			}
			return stopFuture;
		}
	}

}
