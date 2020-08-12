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

package io.activej.common.concurrent;

import io.activej.common.api.WithInitializer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static io.activej.common.Checks.checkArgument;

public final class SimpleThreadFactory implements ThreadFactory, WithInitializer<SimpleThreadFactory> {
	public static final String NAME_PATTERN = "{}";

	private ThreadGroup threadGroup;
	private String name = "Thread-{}";
	private int priority;
	private boolean daemon;

	private final AtomicInteger count = new AtomicInteger(0);

	private SimpleThreadFactory() {
	}

	private SimpleThreadFactory(String name) {
		this.name = name;
	}

	public static SimpleThreadFactory create() {
		return new SimpleThreadFactory();
	}

	public static SimpleThreadFactory create(String name) {
		return new SimpleThreadFactory(name);
	}

	@SuppressWarnings("UnusedReturnValue")
	public SimpleThreadFactory withThreadGroup(ThreadGroup threadGroup) {
		this.threadGroup = threadGroup;
		return this;
	}

	public SimpleThreadFactory withName(String name) {
		this.name = name;
		return this;
	}

	public SimpleThreadFactory withPriority(int priority) {
		checkArgument(priority == 0 || (priority >= Thread.MIN_PRIORITY && priority <= Thread.MAX_PRIORITY),
				() -> "Thread priority should either be 0 or in bounds [" + Thread.MIN_PRIORITY + '-' + Thread.MAX_PRIORITY + ']');
		this.priority = priority;
		return this;
	}

	public SimpleThreadFactory withDaemon(boolean daemon) {
		this.daemon = daemon;
		return this;
	}

	public int getCount() {
		return count.get();
	}

	public void setCount(int count) {
		this.count.set(count);
	}

	public ThreadGroup getThreadGroup() {
		return threadGroup;
	}

	public String getName() {

		return name;
	}

	public int getPriority() {
		return priority;
	}

	public boolean isDaemon() {
		return daemon;
	}

	@Override
	public Thread newThread(@NotNull Runnable runnable) {
		Thread thread = name == null ?
				new Thread(threadGroup, runnable) :
				new Thread(threadGroup, runnable, name.replace(NAME_PATTERN, "" + count.incrementAndGet()));
		thread.setDaemon(daemon);
		if (priority != 0) {
			thread.setPriority(priority);
		}
		return thread;
	}

}
