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

import io.activej.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;

import static java.lang.System.currentTimeMillis;

public final class ExceptionStats implements JmxStats<ExceptionStats>, JmxStatsWithReset {
	private static final long DETAILS_REFRESH_TIMEOUT = 1000L;

	private @Nullable Class<? extends Throwable> exceptionClass;
	private int count;
	private long lastExceptionTimestamp;
	private @Nullable Throwable throwable;
	private @Nullable Object context;

	private ExceptionStats() {
	}

	public static ExceptionStats create() {
		return new ExceptionStats();
	}

	public void recordException(Throwable e, @Nullable Object context) {
		this.count++;
		long now = currentTimeMillis();

		if (now >= lastExceptionTimestamp + DETAILS_REFRESH_TIMEOUT) {
			this.exceptionClass = e != null ? e.getClass() : null;
			this.throwable = e;
			this.context = context;
			this.lastExceptionTimestamp = now;
		}
	}

	public void recordException(Throwable e) {
		recordException(e, null);
	}

	@Override
	public void resetStats() {
		count = 0;
		lastExceptionTimestamp = 0;

		exceptionClass = null;
		throwable = null;
		context = null;
	}

	@Override
	public void add(ExceptionStats another) {
		count += another.count;
		if (another.lastExceptionTimestamp >= lastExceptionTimestamp) {
			lastExceptionTimestamp = another.lastExceptionTimestamp;

			exceptionClass = another.exceptionClass;
			throwable = another.throwable;
			context = another.context;
		}
	}

	@JmxAttribute(optional = true)
	public int getTotal() {
		return count;
	}

	@JmxAttribute(optional = true)
	public @Nullable String getLastType() {
		return exceptionClass != null ? exceptionClass.toString() : null;
	}

	@JmxAttribute(optional = true)
	public @Nullable Instant getLastTime() {
		return lastExceptionTimestamp != 0L ? Instant.ofEpochMilli(lastExceptionTimestamp) : null;
	}

	@JmxAttribute(optional = true)
	public @Nullable Throwable getLastException() {
		return throwable;
	}

	@JmxAttribute(optional = true)
	public @Nullable String getLastMessage() {
		return throwable != null ? throwable.getMessage() : null;
	}

	@JmxAttribute
	public String getMultilineError() {
		if (count == 0) return "";

		StringBuilder summary = new StringBuilder("Count: " + count + " " + MBeanFormat.formatTimestamp(lastExceptionTimestamp));

		if (throwable != null) {
			summary.append("\nStack Trace: ");
			summary.append(MBeanFormat.formatExceptionMultiline(throwable).trim());
		}

		if (context != null) {
			summary.append("\nContext: ");
			summary.append(context);
		}

		return summary.toString();
	}

	@JmxAttribute
	public @Nullable List<String> getError() {
		return MBeanFormat.formatMultilineStringAsList(getMultilineError());
	}

	@Override
	public String toString() {
		String last = "";
		if (exceptionClass != null) {
			last = "; " + exceptionClass.getSimpleName();
			last += " @" + MBeanFormat.formatTimestamp(lastExceptionTimestamp);
		}
		return count + last;
	}

	public @Nullable Object getContext() {
		return context;
	}
}
