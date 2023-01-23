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

public final class ScheduledRunnable implements Comparable<ScheduledRunnable> {
	private final long timestamp;
	private Runnable runnable;
	private boolean cancelled;
	private boolean complete;

	private ScheduledRunnable(long timestamp, Runnable runnable) {
		this.timestamp = timestamp;
		this.runnable = runnable;
	}

	public static ScheduledRunnable create(long timestamp, Runnable runnable) {
		return new ScheduledRunnable(timestamp, runnable);
	}

	@SuppressWarnings("AssignmentToNull") // runnable has been cancelled
	public void cancel() {
		cancelled = true;
		runnable = null;
	}

	@SuppressWarnings("AssignmentToNull") // runnable has been completed
	public void complete() {
		complete = true;
		runnable = null;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Runnable getRunnable() {
		return runnable;
	}

	public boolean isCancelled() {
		return cancelled;
	}

	public boolean isComplete() {
		return complete;
	}

	@Override
	public int compareTo(ScheduledRunnable o) {
		return Long.compare(timestamp, o.timestamp);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ScheduledRunnable that = (ScheduledRunnable) o;
		return timestamp == that.timestamp;
	}

	@Override
	public int hashCode() {
		return (int) (timestamp ^ (timestamp >>> 32));
	}

	@Override
	public String toString() {
		return "ScheduledRunnable{timestamp=" + timestamp + ", cancelled="
				+ cancelled + ", complete=" + complete + ", runnable=" + runnable + '}';
	}
}
