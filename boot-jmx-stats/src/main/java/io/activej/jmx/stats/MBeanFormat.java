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

package io.activej.jmx.stats;

import io.activej.common.StringFormatUtils;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class MBeanFormat {

	public static String formatExceptionMultiline(@Nullable Throwable e) {
		if (e == null) return "";
		StringWriter stringWriter = new StringWriter();
		e.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}

	public static String formatTimestamp(long timestamp) {
		if (timestamp == 0L) return "";
		Instant instant = Instant.ofEpochMilli(timestamp);
		Duration ago = Duration.between(instant, Instant.ofEpochMilli(currentTimeMillis())).withNanos(0);
		return StringFormatUtils.formatInstant(instant) +
				" (" + StringFormatUtils.formatDuration(ago) + " ago)";
	}

	public static String formatListAsMultilineString(@Nullable List<?> list) {
		if (list == null || list.isEmpty()) return "";
		List<String> strings = list.stream().map(Object::toString).collect(toList());
		return (strings.stream().anyMatch(s -> s.contains("\n")) ?
				strings.stream().map(s -> s + "\n") :
				strings.stream())
				.collect(joining("\n")).trim();
	}

	public static @Nullable List<String> formatMultilineStringAsList(@Nullable String multiline) {
		if (multiline == null) return null;
		return multiline.isEmpty() ?
				null :
				Arrays.asList(multiline.split("\n"));
	}
}
