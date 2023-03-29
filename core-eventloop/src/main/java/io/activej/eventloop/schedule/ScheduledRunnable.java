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
	final long timestamp;
	ScheduledPriorityQueue queue;
	int index;
	private final Runnable runnable;

	public ScheduledRunnable(long timestamp, Runnable runnable) {
		this.timestamp = timestamp;
		this.runnable = runnable;
	}

	public void cancel() {
		if (queue != null) {
			queue.remove(this);
		}
	}

	public Runnable runnable() {
		return runnable;
	}

	public long timestamp() {
		return timestamp;
	}

	public boolean isActive() {
		return queue != null;
	}

	@Override
	public String toString() {
		return "{" + Duration.between(Instant.now(), Instant.ofEpochMilli(timestamp)) + " : " + runnable + "}";
	}
}
