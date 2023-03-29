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

package io.activej.eventloop.schedule;

import java.time.Duration;
import java.time.Instant;

public final class ScheduledRunnable {
	private final ScheduledPriorityQueue queue;
	int index;
	private final long timestamp;
	private Runnable runnable;

	// region builders
	public ScheduledRunnable(ScheduledPriorityQueue queue, long timestamp, Runnable runnable) {
		this.queue = queue;
		this.timestamp = timestamp;
		this.runnable = runnable;
	}

	static long compare(ScheduledRunnable x, ScheduledRunnable y) {
		return x.timestamp - y.timestamp;
	}
	// endregion

	@SuppressWarnings("AssignmentToNull") // runnable has been cancelled
	public void cancel() {
		if (runnable != null) {
			queue.remove(this);
			runnable = null;
		}
	}

	public Runnable takeRunnable() {
		Runnable runnable = this.runnable;
		this.runnable = null;
		return runnable;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isActive() {
		return runnable != null;
	}

	@Override
	public String toString() {
		return "{" + Duration.between(Instant.now(), Instant.ofEpochMilli(timestamp)) + " : " + runnable + "}";
	}
}
