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

import io.activej.common.time.CurrentTimeProvider;
import io.activej.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;

public final class RpcConnectStats {
	private final CurrentTimeProvider now;

	private long successfulConnects;
	private long failedConnects;

	private int reconnectAttempts;
	private long connectionLostAt;

	public RpcConnectStats(CurrentTimeProvider now) {
		this.now = now;
	}

	public void reset() {
		successfulConnects = failedConnects = 0;
		reconnectAttempts = 0;
		connectionLostAt = -1;
	}

	public void recordSuccessfulConnect() {
		successfulConnects++;
		reconnectAttempts = 0;
		connectionLostAt = -1;
	}

	public void recordFailedConnect() {
		failedConnects++;
		if (connectionLostAt == -1) {
			connectionLostAt = now.currentTimeMillis();
		} else {
			reconnectAttempts++;
		}
	}

	@JmxAttribute
	public long getSuccessfulConnects() {
		return successfulConnects;
	}

	@JmxAttribute
	public long getFailedConnects() {
		return failedConnects;
	}

	@JmxAttribute
	public boolean isConnected() {
		return connectionLostAt == -1;
	}

	@JmxAttribute
	@Nullable
	public Duration getConnectionLostFor() {
		if (connectionLostAt == -1) return null;

		return Duration.ofMillis(now.currentTimeMillis() - connectionLostAt);
	}

	@JmxAttribute
	@Nullable
	public Instant getConnectionLostAt() {
		if (connectionLostAt == -1) return null;

		return Instant.ofEpochMilli(connectionLostAt);
	}

	@JmxAttribute
	public int getReconnectAttempts() {
		return reconnectAttempts;
	}
}
