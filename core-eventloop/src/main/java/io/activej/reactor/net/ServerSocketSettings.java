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

import io.activej.common.MemSize;
import io.activej.common.initializer.AbstractBuilder;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

import static io.activej.common.Checks.checkState;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;

/**
 * This class used to change settings for server socket. It will be applying with creating new server socket
 */
public final class ServerSocketSettings {
	public static final int DEFAULT_BACKLOG = 16384;

	private static final byte DEF_BOOL = -1;
	private static final byte TRUE = 1;
	private static final byte FALSE = 0;

	private int backlog = DEFAULT_BACKLOG;
	private int receiveBufferSize = 0;
	private byte reuseAddress = DEF_BOOL;

	// region builders
	private ServerSocketSettings() {
	}

	public static ServerSocketSettings create() {
		return builder().build();
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
			ServerSocketSettings.this.receiveBufferSize = receiveBufferSize.toInt();
			return this;
		}

		public Builder withReuseAddress(boolean reuseAddress) {
			checkNotBuilt(this);
			ServerSocketSettings.this.reuseAddress = reuseAddress ? TRUE : FALSE;
			return this;
		}

		@Override
		protected ServerSocketSettings doBuild() {
			return ServerSocketSettings.this;
		}
	}
	// endregion

	public void applySettings(ServerSocketChannel channel) throws IOException {
		if (receiveBufferSize != 0) {
			channel.setOption(SO_RCVBUF, receiveBufferSize);
		}
		if (reuseAddress != DEF_BOOL) {
			channel.setOption(SO_REUSEADDR, reuseAddress != FALSE);
		}
	}

	public int getBacklog() {
		return backlog;
	}

	public boolean hasReceiveBufferSize() {
		return receiveBufferSize != 0;
	}

	public MemSize getReceiveBufferSize() {
		return MemSize.of(getReceiveBufferSizeBytes());
	}

	public int getReceiveBufferSizeBytes() {
		checkState(hasReceiveBufferSize(), "No 'receive buffer size' setting is present");
		return receiveBufferSize;
	}

	public boolean hasReuseAddress() {
		return reuseAddress != DEF_BOOL;
	}

	public boolean getReuseAddress() {
		checkState(hasReuseAddress(), "No 'reuse address' setting is present");
		return reuseAddress != FALSE;
	}

	public Builder asBuilder() {
		ServerSocketSettings serverSocketSettings = new ServerSocketSettings();
		serverSocketSettings.backlog = backlog;
		serverSocketSettings.receiveBufferSize = receiveBufferSize;
		serverSocketSettings.reuseAddress = reuseAddress;
		return serverSocketSettings.new Builder();
	}
}
