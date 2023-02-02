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

package io.activej.launchers.initializers;

import io.activej.async.service.TaskScheduler;
import io.activej.common.MemSize;
import io.activej.common.annotation.StaticFactories;
import io.activej.common.initializer.Initializer;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.http.HttpServer;
import io.activej.inject.Key;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.net.AbstractReactiveServer;
import io.activej.net.PrimaryServer;
import io.activej.trigger.TriggerResult;
import io.activej.trigger.TriggersModuleSettings;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.launchers.initializers.TriggersHelper.ofPromiseStatsLastSuccess;
import static io.activej.trigger.Severity.*;

@StaticFactories(Initializer.class)
public class Initializers {
	public static final String GLOBAL_EVENTLOOP_NAME = "GlobalEventloopStats";
	public static final Key<Eventloop> GLOBAL_EVENTLOOP_KEY = Key.of(Eventloop.class, GLOBAL_EVENTLOOP_NAME);

	public static <S extends AbstractReactiveServer, B extends AbstractReactiveServer.Builder<B, S>> Initializer<B> ofAbstractServer(Config config) {
		return builder -> builder
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "listenAddresses"))
				.withAcceptOnce(config.get(ofBoolean(), "acceptOnce", false))
				.setIfNotNull(
						AbstractReactiveServer.Builder::withSocketSettings,
						config.get(ofSocketSettings(), "socketSettings", null)
				)
				.setIfNotNull(
						AbstractReactiveServer.Builder::withServerSocketSettings,
						config.get(ofServerSocketSettings(), "serverSocketSettings", null)
				);
	}

	public static Initializer<PrimaryServer.Builder> ofPrimaryServer(Config config) {
		return ofAbstractServer(config);
	}

	public static Initializer<Eventloop.Builder> ofEventloop(Config config) {
		return builder -> builder
				.setIfNotNull(
						Eventloop.Builder::withFatalErrorHandler,
						config.get(ofFatalErrorHandler(), "fatalErrorHandler", null)
				)
				.setIfNotNull(
						Eventloop.Builder::withIdleInterval,
						config.get(ofDuration(), "idleInterval", null)
				)
				.setIfNotNull(
						Eventloop.Builder::withThreadPriority,
						config.get(ofInteger(), "threadPriority", null)
				);
	}

	public static Initializer<TaskScheduler.Builder> ofTaskScheduler(Config config) {
		return builder -> builder
				.withEnabled(!config.get(ofBoolean(), "disabled", false))
				.withAbortOnError(config.get(ofBoolean(), "abortOnError", false))
				.withInitialDelay(config.get(ofDuration(), "initialDelay", Duration.ZERO))
				.withSchedule(config.get(ofReactorTaskSchedule(), "schedule"))
				.withRetryPolicy(config.get(ofRetryPolicy(), "retryPolicy"));
	}

	public static Initializer<HttpServer.Builder> ofHttpServer(Config config) {
		return builder -> builder
				.initialize(ofAbstractServer(config))
				.initialize(ofHttpWorker(config));
	}

	public static Initializer<HttpServer.Builder> ofHttpWorker(Config config) {
		return builder -> builder
				.withKeepAliveTimeout(config.get(ofDuration(), "keepAliveTimeout", HttpServer.KEEP_ALIVE_TIMEOUT))
				.withReadWriteTimeout(config.get(ofDuration(), "readWriteTimeout", HttpServer.READ_WRITE_TIMEOUT))
				.withMaxBodySize(config.get(ofMemSize(), "maxBodySize", MemSize.ZERO));
	}

	public static Initializer<JmxModule.Builder> ofGlobalEventloopStats() {
		return builder -> builder
				.withGlobalMBean(Eventloop.class, GLOBAL_EVENTLOOP_KEY)
				.withOptional(GLOBAL_EVENTLOOP_KEY, "fatalErrors_total")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "businessLogicTime_smoothedAverage")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "loops_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "loops_smoothedRate")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "idleLoops_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "idleLoops_smoothedRate")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "selectOverdues_totalCount")
				.withOptional(GLOBAL_EVENTLOOP_KEY, "selectOverdues_smoothedRate");
	}

	public static Initializer<TriggersModuleSettings> ofLauncherTriggers(Duration maxRunDelay) {
		return triggersSettings -> triggersSettings
				.with(Launcher.class, AVERAGE, "runDelay", launcher ->
						TriggerResult.ofValue(launcher.getDurationOfStart(),
								launcher.getInstantOfRun() == null &&
										launcher.getDurationOfStart() != null &&
										launcher.getDurationOfStart().toMillis() > maxRunDelay.toMillis()));
	}

	public static Initializer<TriggersModuleSettings> ofEventloopFatalErrorsTriggers() {
		return triggersSettings -> triggersSettings
				.with(Eventloop.class, HIGH, "fatalErrors", eventloop ->
						eventloop.getStats() == null ?
								TriggerResult.none() :
								TriggerResult.ofError(eventloop.getStats().getFatalErrors()));
	}

	public static Initializer<TriggersModuleSettings> ofEventloopBusinessLogicTriggers(Config config) {
		long businessLogicTimeLow = config.get(ofDurationAsMillis(), "businessLogicTimeLow", 10L);
		long businessLogicTimeHigh = config.get(ofDurationAsMillis(), "businessLogicTimeHigh", 100L);

		return triggersSettings -> triggersSettings
				.with(Eventloop.class, WARNING, "businessLogic", eventloop ->
						eventloop.getStats() == null ?
								TriggerResult.none() :
								TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(), businessLogicTime -> businessLogicTime > businessLogicTimeLow))
				.with(Eventloop.class, HIGH, "businessLogic", eventloop ->
						eventloop.getStats() == null ?
								TriggerResult.none() :
								TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(), businessLogicTime -> businessLogicTime > businessLogicTimeHigh));
	}

	public static Initializer<TriggersModuleSettings> ofThrottlingControllerTriggers(Config config) {
		double throttlingLow = config.get(ofDouble(), "throttlingLow", 0.1);
		double throttlingHigh = config.get(ofDouble(), "throttlingHigh", 0.5);

		return triggersSettings -> triggersSettings
				.with(ThrottlingController.class, WARNING, "throttling", throttlingController ->
						TriggerResult.ofValue(throttlingController == null ? null : throttlingController.getAvgThrottling(),
								throttling -> throttling > throttlingLow))
				.with(ThrottlingController.class, HIGH, "throttling", throttlingController ->
						TriggerResult.ofValue(throttlingController == null ? null : throttlingController.getAvgThrottling(),
								throttling -> throttling > throttlingHigh));
	}

	public static Initializer<TriggersModuleSettings> ofReactorTaskSchedulerTriggers(Config config) {
		Long delayError = config.get(ofDurationAsMillis(), "scheduler.delayError", null);
		Long delayWarning = config.get(ofDurationAsMillis(), "scheduler.delayWarning", null);

		return triggersSettings -> triggersSettings
				.with(TaskScheduler.class, WARNING, "error", scheduler ->
						TriggerResult.ofError(scheduler.getLastException()))
				.with(TaskScheduler.class, INFORMATION, "error", scheduler ->
						ofPromiseStatsLastSuccess(scheduler.getStats()))

				.with(TaskScheduler.class, WARNING, "delay", scheduler -> {
					Duration currentDuration = scheduler.getStats().getCurrentDuration();
					Duration duration = getDuration(scheduler);
					if (currentDuration == null || duration == null) {
						return TriggerResult.none();
					}
					return TriggerResult.ofInstant(scheduler.getStats().getLastStartTime(),
							currentDuration.toMillis() > (delayWarning != null ? delayWarning : duration.toMillis() * 3));
				})
				.with(TaskScheduler.class, HIGH, "delay", scheduler -> {
					Duration currentDuration = scheduler.getStats().getCurrentDuration();
					Duration duration = getDuration(scheduler);
					if (currentDuration == null || duration == null) {
						return TriggerResult.none();
					}
					return TriggerResult.ofInstant(scheduler.getStats().getLastStartTime(),
							currentDuration.toMillis() > (delayError != null ? delayError : duration.toMillis() * 10));
				});
	}

	public static Initializer<JmxModule.Builder> renamedClassNames(Map<Class<?>, String> classToOldName) {
		Map<String, Map<String, String>> byPackageAndClassMap = new HashMap<>();

		for (Map.Entry<Class<?>, String> entry : classToOldName.entrySet()) {
			Class<?> cls = entry.getKey();
			byPackageAndClassMap
					.computeIfAbsent(cls.getPackageName(), $ -> new HashMap<>())
					.put(cls.getSimpleName(), entry.getValue());
		}

		return builder ->
				builder.withObjectNameMapping(protoObjectName -> {
					Map<String, String> packageMap = byPackageAndClassMap.get(protoObjectName.getPackageName());
					if (packageMap == null) return protoObjectName;

					String oldClassName = packageMap.get(protoObjectName.getClassName());

					if (oldClassName == null) return protoObjectName;

					int lastDotIndex = oldClassName.lastIndexOf(".");

					if (lastDotIndex == -1) {
						return protoObjectName.withClassName(oldClassName);
					}

					String packageName = oldClassName.substring(0, lastDotIndex);
					String className = oldClassName.substring(lastDotIndex + 1);

					return protoObjectName
							.withPackageName(packageName)
							.withClassName(className);
				});
	}

	private static Duration getDuration(TaskScheduler scheduler) {
		return scheduler.getPeriod() != null ? scheduler.getPeriod() : scheduler.getInterval();
	}
}
