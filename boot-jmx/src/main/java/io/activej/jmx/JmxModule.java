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

package io.activej.jmx;

import io.activej.bytebuf.ByteBufPool;
import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.common.StringFormatUtils;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.Initializer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.BindingType;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.jmx.DynamicMBeanFactory.JmxCustomTypeAdapter;
import io.activej.jmx.stats.JmxHistogram;
import io.activej.jmx.stats.ValueStats;
import io.activej.launcher.LauncherService;
import io.activej.trigger.Severity;
import io.activej.trigger.Triggers.TriggerWithResult;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPools;
import org.jetbrains.annotations.Nullable;

import javax.management.DynamicMBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Turns on support of Jmx in application.
 * <br>
 * Automatically builds MBeans for parts of application and adds Jmx attributes and operations to it.
 */
public final class JmxModule extends AbstractModule {
	public static final Duration REFRESH_PERIOD_DEFAULT = ApplicationSettings.getDuration(JmxModule.class, "refreshPeriod", Duration.ofSeconds(1));
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = ApplicationSettings.getInt(JmxModule.class, "maxJmxRefreshesPerOneCycle", 50);

	private final Set<Object> globalSingletons = new HashSet<>();

	private final Map<Key<?>, JmxBeanSettings> keyToSettings = new HashMap<>();
	private final Map<Type, JmxBeanSettings> typeToSettings = new HashMap<>();
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes = new HashMap<>();
	private final Map<Type, Key<?>> globalMBeans = new HashMap<>();

	private Duration refreshPeriod = REFRESH_PERIOD_DEFAULT;
	private int maxJmxRefreshesPerOneCycle = MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT;
	private ProtoObjectNameMapper objectNameMapper = ProtoObjectNameMapper.identity();
	private BiPredicate<Key<?>, Integer> workerPredicate = (key, workerId) -> true;
	private boolean withScopes = true;

	private JmxModule() {
	}

	public static JmxModule create() {
		return builder().build();
	}

	public static Builder builder() {
		return new JmxModule().new Builder()
				.withCustomType(Duration.class, StringFormatUtils::formatDuration, StringFormatUtils::parseDuration)
				.withCustomType(Period.class, StringFormatUtils::formatPeriod, StringFormatUtils::parsePeriod)
				.withCustomType(Instant.class, StringFormatUtils::formatInstant, StringFormatUtils::parseInstant)
				.withCustomType(LocalDateTime.class, StringFormatUtils::formatLocalDateTime, StringFormatUtils::parseLocalDateTime)
				.withCustomType(MemSize.class, StringFormatUtils::formatMemSize, StringFormatUtils::parseMemSize)
				.withCustomType(TriggerWithResult.class, TriggerWithResult::toString)
				.withCustomType(Severity.class, Severity::toString)
				.withCustomType(InetSocketAddress.class, StringFormatUtils::formatInetSocketAddress, addressString -> {
					try {
						return StringFormatUtils.parseInetSocketAddress(addressString);
					} catch (MalformedDataException e) {
						throw new IllegalArgumentException(e);
					}
				})
				.withGlobalSingletons(ByteBufPool.getStats());
	}

	public final class Builder extends AbstractBuilder<Builder, JmxModule> {
		private final Map<Key<?>, JmxBeanSettings.Builder> keyToSettingsBuilder = new HashMap<>();
		private final Map<Type, JmxBeanSettings.Builder> typeToSettingsBuilder = new HashMap<>();

		private Builder() {}

		public Builder withRefreshPeriod(Duration refreshPeriod) {
			checkNotBuilt(this);
			checkArgument(refreshPeriod.toMillis() > 0, "Duration of refresh period should be a positive value");
			JmxModule.this.refreshPeriod = refreshPeriod;
			return this;
		}

		public Builder withMaxJmxRefreshesPerOneCycle(int max) {
			checkNotBuilt(this);
			checkArgument(max > 0, "Number of JMX refreshes should be a positive value");
			JmxModule.this.maxJmxRefreshesPerOneCycle = max;
			return this;
		}

		public <T> Builder withModifier(Key<?> key, String attrName, AttributeModifier<T> modifier) {
			checkNotBuilt(this);
			keyToSettingsBuilder.computeIfAbsent(key, $ -> JmxBeanSettings.builder())
					.withModifier(attrName, modifier);
			return this;
		}

		public <T> Builder withModifier(Type type, String attrName, AttributeModifier<T> modifier) {
			checkNotBuilt(this);
			typeToSettingsBuilder.computeIfAbsent(type, $ -> JmxBeanSettings.builder())
					.withModifier(attrName, modifier);
			return this;
		}

		public Builder withOptional(Key<?> key, String attrName) {
			checkNotBuilt(this);
			keyToSettingsBuilder.computeIfAbsent(key, $ -> JmxBeanSettings.builder())
					.withIncludedOptional(attrName);
			return this;
		}

		public Builder withOptional(Type type, String attrName) {
			checkNotBuilt(this);
			typeToSettingsBuilder.computeIfAbsent(type, $ -> JmxBeanSettings.builder())
					.withIncludedOptional(attrName);
			return this;
		}

		public Builder withHistogram(Class<?> clazz, String attrName, int[] histogramLevels) {
			checkNotBuilt(this);
			return withHistogram(Key.of(clazz), attrName, () -> JmxHistogram.ofLevels(histogramLevels));
		}

		public Builder withHistogram(Key<?> key, String attrName, int[] histogramLevels) {
			checkNotBuilt(this);
			return withHistogram(key, attrName, () -> JmxHistogram.ofLevels(histogramLevels));
		}

		public Builder withHistogram(Class<?> clazz, String attrName, Supplier<JmxHistogram> histogram) {
			checkNotBuilt(this);
			return withHistogram(Key.of(clazz), attrName, histogram);
		}

		public Builder withHistogram(Key<?> key, String attrName, Supplier<JmxHistogram> histogram) {
			checkNotBuilt(this);
			return withOptional(key, attrName + "_histogram")
					.withModifier(key, attrName, (ValueStats attribute) ->
							attribute.setHistogram(histogram.get()));
		}

		public Builder withGlobalMBean(Type type, String named) {
			checkNotBuilt(this);
			return withGlobalMBean(type, Key.ofType(type, named));
		}

		public Builder withGlobalMBean(Type type, Key<?> key) {
			checkNotBuilt(this);
			checkArgument(key.getType() == type, "Type " + type + " does not match key type " + key.getType());

			globalMBeans.put(type, key);
			return this;
		}

		public Builder withObjectNameMapping(ProtoObjectNameMapper objectNameMapper) {
			checkNotBuilt(this);
			JmxModule.this.objectNameMapper = objectNameMapper;
			return this;
		}

		public Builder withScopes(boolean withScopes) {
			checkNotBuilt(this);
			JmxModule.this.withScopes = withScopes;
			return this;
		}

		public <T> Builder withCustomType(Class<T> type, Function<T, String> to, Function<String, T> from) {
			checkNotBuilt(this);
			JmxModule.this.customTypes.put(type, new JmxCustomTypeAdapter<>(to, from));
			return this;
		}

		public <T> Builder withCustomType(Class<T> type, Function<T, String> to) {
			checkNotBuilt(this);
			JmxModule.this.customTypes.put(type, new JmxCustomTypeAdapter<>(to));
			return this;
		}

		public Builder withGlobalSingletons(Object... instances) {
			checkNotBuilt(this);
			checkArgument(Arrays.stream(instances).map(Object::getClass).noneMatch(Class::isAnonymousClass),
					"Instances of anonymous classes will not be registered in JMX");
			JmxModule.this.globalSingletons.addAll(List.of(instances));
			return this;
		}

		public Builder withWorkerPredicate(BiPredicate<Key<?>, Integer> predicate) {
			checkNotBuilt(this);
			JmxModule.this.workerPredicate = predicate;
			return this;
		}

		@Override
		protected JmxModule doBuild() {
			keyToSettings.putAll(keyToSettingsBuilder.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build())));
			typeToSettings.putAll(typeToSettingsBuilder.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build())));
			return JmxModule.this;
		}
	}

	@Provides
	JmxRegistry jmxRegistry(DynamicMBeanFactory mbeanFactory) {
		return JmxRegistry.builder(ManagementFactory.getPlatformMBeanServer(), mbeanFactory)
				.withCustomTypes(customTypes)
				.withObjectNameMapping(objectNameMapper)
				.withScopes(withScopes)
				.withWorkerPredicate(workerPredicate)
				.build();
	}

	@Provides
	DynamicMBeanFactory mbeanFactory() {
		return DynamicMBeanFactory.create(refreshPeriod, maxJmxRefreshesPerOneCycle);
	}

	@ProvidesIntoSet
	LauncherService service(Injector injector, JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory, OptionalDependency<Set<Initializer<JmxModule>>> initializers) {
		for (Initializer<JmxModule> initializer : initializers.orElse(Set.of())) {
			initializer.initialize(this);
		}
		return new LauncherService() {
			@Override
			public CompletableFuture<?> start() {
				doStart(injector, jmxRegistry, mbeanFactory);
				return completedFuture(null);
			}

			@Override
			public CompletableFuture<?> stop() {
				jmxRegistry.unregisterAll();
				return completedFuture(null);
			}
		};
	}

	private void doStart(Injector injector, JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory) {
		Map<Type, Set<Object>> globalMBeanObjects = new HashMap<>();

		// register global singletons
		for (Object globalSingleton : globalSingletons) {
			Key<?> globalKey = Key.of(globalSingleton.getClass());
			registerSingleton(jmxRegistry, globalSingleton, globalKey, injector,
					JmxBeanSettings.builder()
							.withCustomTypes(customTypes)
							.build());
		}

		// register singletons
		for (Map.Entry<Key<?>, Object> entry : injector.peekInstances().entrySet()) {
			Key<?> key = entry.getKey();
			Object instance = entry.getValue();
			if (instance == null || key.getRawType().isAnonymousClass()) continue;
			registerSingleton(jmxRegistry, instance, key, injector, null);

			Type type = key.getType();
			if (globalMBeans.containsKey(type)) {
				globalMBeanObjects.computeIfAbsent(type, $ -> newSetFromMap(new IdentityHashMap<>())).add(instance);
			}
		}

		// register workers
		WorkerPools workerPools = injector.peekInstance(WorkerPools.class);
		if (workerPools != null) {
			// populating workerPoolKeys map
			injector.peekInstances().entrySet().stream()
					.filter(entry -> entry.getKey().getRawType().equals(WorkerPool.class))
					.forEach(entry -> jmxRegistry.addWorkerPoolKey((WorkerPool) entry.getValue(), entry.getKey()));

			for (WorkerPool workerPool : workerPools.getWorkerPools()) {
				Injector[] scopeInjectors = workerPool.getScopeInjectors();
				if (scopeInjectors.length == 0) continue;
				Injector workerScopeInjector = scopeInjectors[0];
				for (Map.Entry<Key<?>, WorkerPool.Instances<?>> entry : workerPool.peekInstances().entrySet()) {
					Key<?> key = entry.getKey();
					WorkerPool.Instances<?> workerInstances = entry.getValue();
					if (key.getRawType().isAnonymousClass()) continue;
					registerWorkers(jmxRegistry, workerPool, key, workerInstances.getList(), workerScopeInjector);

					Type type = key.getType();
					if (globalMBeans.containsKey(type)) {
						for (Object instance : workerInstances) {
							globalMBeanObjects.computeIfAbsent(type, $ -> newSetFromMap(new IdentityHashMap<>())).add(instance);
						}
					}
				}
			}
		}

		for (Map.Entry<Type, Set<Object>> entry : globalMBeanObjects.entrySet()) {
			Key<?> key = globalMBeans.get(entry.getKey());
			DynamicMBean globalMBean =
					mbeanFactory.createDynamicMBean(new ArrayList<>(entry.getValue()), ensureSettingsFor(key), false);
			registerSingleton(jmxRegistry, globalMBean, key, injector, JmxBeanSettings.create());
		}
	}

	private void registerSingleton(JmxRegistry jmxRegistry, Object instance, Key<?> key, Injector injector, @Nullable JmxBeanSettings settings) {
		if (key.getRawType() == OptionalDependency.class) {
			OptionalDependency<?> optional = (OptionalDependency<?>) instance;
			if (!optional.isPresent()) return;
			Binding<?> binding = injector.getBinding(key);
			if (binding == null || binding.getType() == BindingType.SYNTHETIC) {
				return;
			}
			key = key.getTypeParameter(0).qualified(key.getQualifier());
			instance = optional.get();
		}

		jmxRegistry.registerSingleton(key, instance, settings != null ? settings : ensureSettingsFor(key));
	}

	private void registerWorkers(JmxRegistry jmxRegistry, WorkerPool workerPool, Key<?> key, List<?> workerInstances, Injector injector) {
		int size = workerInstances.size();
		if (size == 0) return;

		if (key.getRawType() == OptionalDependency.class) {
			Binding<?> binding = injector.getBinding(key);
			if (binding == null || binding.getType() == BindingType.SYNTHETIC) {
				return;
			}
			List<Object> instances = new ArrayList<>(size);
			for (Object workerInstance : workerInstances) {
				OptionalDependency<?> optional = (OptionalDependency<?>) workerInstance;
				if (!optional.isPresent()) {
					JmxRegistry.logger.info("Pool of instances with key {} was not registered to jmx, " +
							"because some instances were not present", key);
					return;
				}

				instances.add(optional.get());
			}
			key = key.getTypeParameter(0).qualified(key.getQualifier());
			workerInstances = instances;
		}

		jmxRegistry.registerWorkers(workerPool, key, workerInstances, ensureSettingsFor(key));
	}

	private JmxBeanSettings ensureSettingsFor(Key<?> key) {
		JmxBeanSettings settings = JmxBeanSettings.builder()
				.withCustomTypes(customTypes)
				.build();
		if (keyToSettings.containsKey(key)) {
			settings.merge(keyToSettings.get(key));
		}
		if (typeToSettings.containsKey(key.getType())) {
			settings.merge(typeToSettings.get(key.getType()));
		}
		return settings;
	}
}
