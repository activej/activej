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

import ch.qos.logback.classic.Level;
import org.jetbrains.annotations.Nullable;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

/**
 * {@link TestRule} that enables deeper logger levels for specific tests that request it.
 */
public final class LoggingRule implements TestRule {
	private static final Level DEFAULT_LOGGING_LEVEL = Level.TRACE;

	private interface AnnotationExtractor {
		<A extends Annotation> @Nullable A get(Class<A> annotation);
	}

	private static List<LoggerConfig> getAnnotations(AnnotationExtractor fn) {
		LoggerConfig single = fn.get(LoggerConfig.class);
		if (single == null) {
			LoggerConfig.Container container = fn.get(LoggerConfig.Container.class);
			if (container == null) {
				return emptyList();
			}
			return asList(container.value());
		} else {
			return singletonList(single);
		}
	}

	@Override
	public Statement apply(Statement base, Description description) {
		List<LoggerConfig> clauses = new ArrayList<>();
		clauses.addAll(getAnnotations(description.getTestClass()::getAnnotation));
		clauses.addAll(getAnnotations(description::getAnnotation));
		return new LambdaStatement(() -> {
			setLoggerLevel(ROOT_LOGGER_NAME, DEFAULT_LOGGING_LEVEL);
			Level[] oldLevels = new Level[clauses.size()];
			Logger[] loggers = new Logger[clauses.size()];

			for (int i = 0; i < clauses.size(); i++) {
				LoggerConfig clause = clauses.get(i);
				Logger logger = LoggerFactory.getLogger(
						clause.logger() != Void.class ?
								clause.logger().getName() :
								clause.packageOf() != Void.class ?
										clause.packageOf().getPackage().getName() :
										ROOT_LOGGER_NAME);
				oldLevels[i] = getLoggerLevel(logger);
				loggers[i] = logger;
				setLoggerLevel(logger, getAdaptedLevel(clause.value()));
			}

			try {
				base.evaluate();
			} finally {
				for (int i = 0; i < loggers.length; i++) {
					setLoggerLevel(loggers[i], oldLevels[i]);
				}
			}
		});
	}

	private Level getAdaptedLevel(org.slf4j.event.Level level) {
		switch (level) {
			case ERROR:
				return Level.ERROR;
			case WARN:
				return Level.WARN;
			case INFO:
				return Level.INFO;
			case DEBUG:
				return Level.DEBUG;
			case TRACE:
				return Level.TRACE;
			default:
				return DEFAULT_LOGGING_LEVEL;
		}
	}

	private static Level getLoggerLevel(Logger logger) {
		return ((ch.qos.logback.classic.Logger) logger).getLevel();
	}

	private static void setLoggerLevel(String name, Level level) {
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(name)).setLevel(level);
	}

	private static void setLoggerLevel(Logger logger, Level level) {
		((ch.qos.logback.classic.Logger) logger).setLevel(level);
	}

	public static void enableOfPackageLogging(Class<?> cls) {
		setLoggerLevel(cls.getPackage().getName(), DEFAULT_LOGGING_LEVEL);
	}

	public static void enableOfLoggerLogging(Class<?> cls) {
		setLoggerLevel(cls.getName(), DEFAULT_LOGGING_LEVEL);
	}

	public static void enableOfPackageLogging(Class<?> cls, Level level) {
		setLoggerLevel(cls.getPackage().getName(), level);
	}

	public static void enableOfLoggerLogging(Class<?> cls, Level level) {
		setLoggerLevel(cls.getName(), level);
	}

	public static void enableLogging() {
		setLoggerLevel(ROOT_LOGGER_NAME, DEFAULT_LOGGING_LEVEL);
	}

	public static void enableLogging(Level level) {
		setLoggerLevel(ROOT_LOGGER_NAME, level);
	}
}
