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

package io.activej.reactor.schedule;

import io.activej.common.StringFormatUtils;

import java.time.Instant;

public abstract class ScheduledRunnable implements Runnable {
	final long timestamp;
	ScheduledPriorityQueue queue;
	int index;

	public ScheduledRunnable(long timestamp) {
		this.timestamp = timestamp;
	}

	public static ScheduledRunnable of(long timestamp, Runnable runnable) {
		return new ScheduledRunnableImpl(timestamp, runnable);
	}

	private static final class ScheduledRunnableImpl extends ScheduledRunnable {
		private final Runnable runnable;

		public ScheduledRunnableImpl(long timestamp, Runnable runnable) {
			super(timestamp);
			this.runnable = runnable;
		}

		@Override
		public void run() {
			runnable.run();
		}

		@Override
		protected String runnableToString() {
			return runnable.toString();
		}
	}

	public void cancel() {
		if (queue != null) {
			queue.remove(this);
		}
	}

	public long timestamp() {
		return timestamp;
	}

	public boolean isActive() {
		return queue != null;
	}

	protected String timestampToString() {
		return StringFormatUtils.formatInstant(Instant.ofEpochMilli(timestamp));
	}

	protected String runnableToString() {
		return super.toString();
	}

	@Override
	public String toString() {
		return "{ " + (queue != null ? index : "-") + " @ " + timestampToString() + " : " + runnableToString() + " }";
	}
}
