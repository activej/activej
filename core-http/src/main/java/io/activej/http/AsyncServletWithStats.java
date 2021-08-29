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

package io.activej.http;

import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

public abstract class AsyncServletWithStats implements AsyncServlet, EventloopJmxBeanWithStats {
	@NotNull
	protected final Eventloop eventloop;

	private final PromiseStats stats = PromiseStats.create(Duration.ofMinutes(5));

	protected AsyncServletWithStats(@NotNull Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@NotNull
	protected abstract Promise<HttpResponse> doServe(@NotNull HttpRequest request);

	@NotNull
	@Override
	public final Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return doServe(request)
				.whenComplete(stats.recordStats());
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public PromiseStats getStats() {
		return stats;
	}

	public void setStatsHistogramLevels(int[] levels) {
		stats.setHistogram(levels);
	}

}
