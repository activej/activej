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

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.common.MemSize;
import io.activej.common.initializer.Initializer;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.http.AsyncHttpServer;
import io.activej.inject.Key;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.net.AbstractServer;
import io.activej.net.PrimaryServer;
import io.activej.trigger.TriggerResult;
import io.activej.trigger.TriggersModuleSettings;

import java.time.Duration;

import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.launchers.initializers.TriggersHelper.ofPromiseStatsLastSuccess;
import static io.activej.trigger.Severity.*;

public class Initializers {
	public static final String GLOBAL_EVENTLOOP_NAME = "GlobalEventloopStats";
	public static final Key<Eventloop> GLOBAL_EVENTLOOP_KEY = Key.of(Eventloop.class, GLOBAL_EVENTLOOP_NAME);

	public static <T extends AbstractServer<T>> Initializer<T> ofAbstractServer(Config config) {
		return server -> server
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "listenAddresses"))
				.withAcceptOnce(config.get(ofBoolean(), "acceptOnce", false))
				.withSocketSettings(config.get(ofSocketSettings(), "socketSettings", server.getSocketSettings()))
				.withServerSocketSettings(config.get(ofServerSocketSettings(), "serverSocketSettings", server.getServerSocketSettings()));
	}

	public static Initializer<PrimaryServer> ofPrimaryServer(Config config) {
		return ofAbstractServer(config);
	}

	public static Initializer<Eventloop> ofEventloop(Config config) {
		return eventloop -> eventloop
				.withFatalErrorHandler(config.get(ofFatalErrorHandler(eventloop), "fatalErrorHandler", eventloop.getFatalErrorHandler()))
				.withFatalErrorHandlerThreadLocal(config.get(ofFatalErrorHandler(eventloop), "fatalErrorHandlerThreadLocal", eventloop.getFatalErrorHandlerThreadLocal()))
				.withIdleInterval(config.get(ofDuration(), "idleInterval", eventloop.getIdleInterval()))
				.withThreadPriority(config.get(ofInteger(), "threadPriority", eventloop.getThreadPriority()));
	}

	public static Initializer<EventloopTaskScheduler> ofEventloopTaskScheduler(Config config) {
		return scheduler -> {
			scheduler.setEnabled(!config.get(ofBoolean(), "disabled", false));
			scheduler.withAbortOnError(config.get(ofBoolean(), "abortOnError", false))
					.withInitialDelay(config.get(ofDuration(), "initialDelay", Duration.ZERO))
					.withSchedule(config.get(ofEventloopTaskSchedule(), "schedule", null))
					.withRetryPolicy(config.get(ofRetryPolicy(), "retryPolicy"));
		};
	}

	public static Initializer<AsyncHttpServer> ofHttpServer(Config config) {
		return server -> server
				.withInitializer(ofAbstractServer(config))
				.withInitializer(ofHttpWorker(config));
	}

	public static Initializer<AsyncHttpServer> ofHttpWorker(Config config) {
		return server -> server
				.withKeepAliveTimeout(config.get(ofDuration(), "keepAliveTimeout", server.getKeepAliveTimeout()))
				.withReadWriteTimeout(config.get(ofDuration(), "readWriteTimeout", server.getReadWriteTimeout()))
				.withMaxBodySize(config.get(ofMemSize(), "maxBodySize", MemSize.ZERO));
	}

	public static Initializer<JmxModule> ofGlobalEventloopStats() {
		return jmxModule -> jmxModule
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

	public static Initializer<TriggersModuleSettings> ofEventloopTaskSchedulerTriggers(Config config) {
		Long delayError = config.get(ofDurationAsMillis(), "scheduler.delayError", null);
		Long delayWarning = config.get(ofDurationAsMillis(), "scheduler.delayWarning", null);

		return triggersSettings -> triggersSettings
				.with(EventloopTaskScheduler.class, WARNING, "error", scheduler ->
						TriggerResult.ofError(scheduler.getLastException()))
				.with(EventloopTaskScheduler.class, INFORMATION, "error", scheduler ->
						ofPromiseStatsLastSuccess(scheduler.getStats()))

				.with(EventloopTaskScheduler.class, WARNING, "delay", scheduler -> {
					Duration currentDuration = scheduler.getStats().getCurrentDuration();
					Duration duration = getDuration(scheduler);
					if (currentDuration == null || duration == null) {
						return TriggerResult.none();
					}
					return TriggerResult.ofInstant(scheduler.getStats().getLastStartTime(),
							currentDuration.toMillis() > (delayWarning != null ? delayWarning : duration.toMillis() * 3));
				})
				.with(EventloopTaskScheduler.class, HIGH, "delay", scheduler -> {
					Duration currentDuration = scheduler.getStats().getCurrentDuration();
					Duration duration = getDuration(scheduler);
					if (currentDuration == null || duration == null) {
						return TriggerResult.none();
					}
					return TriggerResult.ofInstant(scheduler.getStats().getLastStartTime(),
							currentDuration.toMillis() > (delayError != null ? delayError : duration.toMillis() * 10));
				});
	}

	private static Duration getDuration(EventloopTaskScheduler scheduler) {
		return scheduler.getPeriod() != null ? scheduler.getPeriod() : scheduler.getInterval();
	}
}
