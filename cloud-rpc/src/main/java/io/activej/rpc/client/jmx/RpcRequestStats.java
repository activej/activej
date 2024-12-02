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

package io.activej.rpc.client.jmx;

import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.jmx.stats.LongValueStats;

import java.time.Duration;

import static io.activej.jmx.stats.JmxHistogram.POWERS_OF_TWO;

public final class RpcRequestStats implements JmxRefreshable {
	private final EventStats totalRequests;
	private final EventStats failedRequests;
	private final EventStats rejectedRequests;
	private final EventStats expiredRequests;
	private final LongValueStats responseTime;
	private final LongValueStats overdues;
	private final ExceptionStats serverExceptions;

	private RpcRequestStats(Duration smoothingWindow) {
		totalRequests = EventStats.create(smoothingWindow);
		failedRequests = EventStats.create(smoothingWindow);
		rejectedRequests = EventStats.create(smoothingWindow);
		expiredRequests = EventStats.create(smoothingWindow);
		responseTime = LongValueStats.builder(smoothingWindow)
			.withHistogram(POWERS_OF_TWO)
			.withUnit("milliseconds")
			.build();
		overdues = LongValueStats.builder(smoothingWindow)
			.withHistogram(POWERS_OF_TWO)
			.withRate()
			.withUnit("milliseconds")
			.build();
		serverExceptions = ExceptionStats.create();
	}

	public static RpcRequestStats create(Duration smoothingWindow) {
		return new RpcRequestStats(smoothingWindow);
	}

	@Override
	public void refresh(long timestamp) {
		totalRequests.refresh(timestamp);
		failedRequests.refresh(timestamp);
		rejectedRequests.refresh(timestamp);
		expiredRequests.refresh(timestamp);
		responseTime.refresh(timestamp);
		overdues.refresh(timestamp);
	}

	@JmxAttribute(extraSubAttributes = "totalCount")
	public EventStats getTotalRequests() {
		return totalRequests;
	}

	@JmxAttribute(extraSubAttributes = "totalCount")
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute(extraSubAttributes = "totalCount")
	public EventStats getRejectedRequests() {
		return rejectedRequests;
	}

	@JmxAttribute(extraSubAttributes = "totalCount")
	public EventStats getExpiredRequests() {
		return expiredRequests;
	}

	@JmxAttribute(
		description = "delay between successful or failed request/response (in milliseconds)",
		extraSubAttributes = "histogram"
	)
	public LongValueStats getResponseTime() {
		return responseTime;
	}

	@JmxAttribute(
		description =
			"difference between due time and actual time of passing response to callback for " +
			"successful or failed requests",
		extraSubAttributes = "histogram"
	)
	public LongValueStats getOverdues() {
		return overdues;
	}

	@JmxAttribute
	public ExceptionStats getServerExceptions() {
		return serverExceptions;
	}
}
