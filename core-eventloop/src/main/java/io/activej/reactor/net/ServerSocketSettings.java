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
import java.nio.channels.ServerSocketChannel;

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;

/**
 * This class used to change settings for server socket. It will be applying with creating new server socket
 */
public final class ServerSocketSettings {
	private static final ServerSocketSettings DEFAULT_INSTANCE = builder().build();

	public static final int DEFAULT_BACKLOG = ApplicationSettings.getInt(ServerSocketSettings.class, "backlog", 16384);
	public static final @Nullable MemSize DEFAULT_RECEIVE_BUFFER_SIZE = ApplicationSettings.getMemSize(ServerSocketSettings.class, "receiveBufferSize", null);
	public static final @Nullable Boolean DEFAULT_REUSE_ADDRESS = ApplicationSettings.getBoolean(ServerSocketSettings.class, "reuseAddress", null);

	private int backlog = DEFAULT_BACKLOG;
	private @Nullable MemSize receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
	private @Nullable Boolean reuseAddress = DEFAULT_REUSE_ADDRESS;

	private ServerSocketSettings() {
	}

	public static ServerSocketSettings create(int backlog) {
		return builder().withBacklog(backlog).build();
	}

	public static ServerSocketSettings defaultInstance() {
		return DEFAULT_INSTANCE;
	}

	public static Builder builder() {
		return new ServerSocketSettings().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ServerSocketSettings> {
		private Builder() {}

		public Builder withBacklog(int backlog) {
			checkNotBuilt(this);
			ServerSocketSettings.this.backlog = backlog;
			return this;
		}

		public Builder withReceiveBufferSize(MemSize receiveBufferSize) {
			checkNotBuilt(this);
			ServerSocketSettings.this.receiveBufferSize = receiveBufferSize;
			return this;
		}

		public Builder withReuseAddress(boolean reuseAddress) {
			checkNotBuilt(this);
			ServerSocketSettings.this.reuseAddress = reuseAddress;
			return this;
		}

		@Override
		protected ServerSocketSettings doBuild() {
			return ServerSocketSettings.this;
		}
	}

	public void applySettings(ServerSocketChannel channel) throws IOException {
		if (receiveBufferSize != null) {
			channel.setOption(SO_RCVBUF, receiveBufferSize.toInt());
		}
		if (reuseAddress != null) {
			channel.setOption(SO_REUSEADDR, reuseAddress);
		}
	}

	public int getBacklog() {
		return backlog;
	}

	public @Nullable MemSize getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public @Nullable Boolean getReuseAddress() {
		return reuseAddress;
	}

}
