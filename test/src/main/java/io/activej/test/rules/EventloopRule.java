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

package io.activej.test.rules;

import io.activej.eventloop.Eventloop;
import io.activej.common.exception.FatalErrorHandlers;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static ch.qos.logback.classic.Level.WARN;

/**
 * {@link TestRule} that creates an eventloop and sets it to ThreadLocal
 */
public final class EventloopRule implements TestRule {

	static {
		createEventloop();
		LoggingRule.enableOfLoggerLogging(Eventloop.class, WARN);
	}

	private static void createEventloop() {
		Eventloop.create()
				.withCurrentThread()
				.withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Override
	public Statement apply(Statement base, Description description) {
		if (!Eventloop.getCurrentEventloop().inEventloopThread()) {
			createEventloop();
		}
		return base;
	}
}
