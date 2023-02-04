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

package io.activej.reactor.net;

import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.common.builder.AbstractBuilder;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.channels.DatagramChannel;

import static java.net.StandardSocketOptions.*;

/**
 * This class is used to change settings for datagram socket. It will be applied when creating a new socket
 */
public final class DatagramSocketSettings {
	public static final @Nullable MemSize DEFAULT_RECEIVE_BUFFER_SIZE = ApplicationSettings.getMemSize(DatagramSocketSettings.class, "receiveBufferSize", null);
	public static final @Nullable MemSize DEFAULT_SEND_BUFFER_SIZE = ApplicationSettings.getMemSize(DatagramSocketSettings.class, "sendBufferSize", null);
	public static final @Nullable Boolean DEFAULT_REUSE_ADDRESS = ApplicationSettings.getBoolean(DatagramSocketSettings.class, "reuseAddress", null);
	public static final @Nullable Boolean DEFAULT_BROADCAST = ApplicationSettings.getBoolean(DatagramSocketSettings.class, "broadcast", null);

	private @Nullable MemSize receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
	private @Nullable MemSize sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
	private @Nullable Boolean reuseAddress = DEFAULT_REUSE_ADDRESS;
	private @Nullable Boolean broadcast = DEFAULT_BROADCAST;

	private DatagramSocketSettings() {
	}

	public static DatagramSocketSettings create() {
		return builder().build();
	}

	public static Builder builder() {
		return new DatagramSocketSettings().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, DatagramSocketSettings> {
		private Builder() {}

		public Builder withReceiveBufferSize(@Nullable MemSize receiveBufferSize) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.receiveBufferSize = receiveBufferSize;
			return this;
		}

		public Builder withSendBufferSize(@Nullable MemSize sendBufferSize) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.sendBufferSize = sendBufferSize;
			return this;
		}

		public Builder withReuseAddress(@Nullable Boolean reuseAddress) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.reuseAddress = reuseAddress;
			return this;
		}

		public Builder withBroadcast(@Nullable Boolean broadcast) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.broadcast = broadcast;
			return this;
		}

		@Override
		protected DatagramSocketSettings doBuild() {
			return DatagramSocketSettings.this;
		}
	}

	public void applySettings(DatagramChannel channel) throws IOException {
		if (receiveBufferSize != null) {
			channel.setOption(SO_RCVBUF, receiveBufferSize.toInt());
		}
		if (sendBufferSize != null) {
			channel.setOption(SO_SNDBUF, sendBufferSize.toInt());
		}
		if (reuseAddress != null) {
			channel.setOption(SO_REUSEADDR, reuseAddress);
		}
		if (broadcast != null) {
			channel.setOption(SO_BROADCAST, broadcast);
		}
	}

	public @Nullable MemSize getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public @Nullable Boolean getReuseAddress() {
		return reuseAddress;
	}

	public @Nullable MemSize getSendBufferSize() {
		return sendBufferSize;
	}

	public @Nullable Boolean getBroadcast() {
		return broadcast;
	}
}
