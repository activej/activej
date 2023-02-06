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
import java.nio.channels.SocketChannel;
import java.time.Duration;

import static java.net.StandardSocketOptions.*;

/**
 * This class is used to hold settings for a socket. Settings  will be applied when creating new socket
 */
public final class SocketSettings {
	private static final SocketSettings DEFAULT_INSTANCE = builder().withTcpNoDelay(true).build();

	public static final @Nullable Boolean DEFAULT_KEEP_ALIVE = ApplicationSettings.getBoolean(SocketSettings.class, "keepAlive");
	public static final @Nullable Boolean DEFAULT_REUSE_ADDRESS = ApplicationSettings.getBoolean(SocketSettings.class, "reuseAddress");
	public static final @Nullable Boolean DEFAULT_TCP_NO_DELAY = ApplicationSettings.getBoolean(SocketSettings.class, "tcpNoDelay");
	public static final @Nullable MemSize DEFAULT_SEND_BUFFER_SIZE = ApplicationSettings.getMemSize(SocketSettings.class, "sendBufferSize", null);
	public static final @Nullable MemSize DEFAULT_RECEIVE_BUFFER_SIZE = ApplicationSettings.getMemSize(SocketSettings.class, "receiveBufferSize", null);
	public static final @Nullable Duration DEFAULT_IMPL_READ_TIMEOUT = ApplicationSettings.getDuration(SocketSettings.class, "implReadTimeout", null);
	public static final @Nullable Duration DEFAULT_IMPL_WRITE_TIMEOUT = ApplicationSettings.getDuration(SocketSettings.class, "implWriteTimeout", null);
	public static final @Nullable MemSize DEFAULT_IMPL_READ_BUFFER_SIZE = ApplicationSettings.getMemSize(SocketSettings.class, "implReadBufferSize", null);
	public static final @Nullable Duration DEFAULT_LINGER_TIMEOUT = ApplicationSettings.getDuration(SocketSettings.class, "lingerTimeout", null);

	private @Nullable Boolean keepAlive = DEFAULT_KEEP_ALIVE;
	private @Nullable Boolean reuseAddress = DEFAULT_REUSE_ADDRESS;
	private @Nullable Boolean tcpNoDelay = DEFAULT_TCP_NO_DELAY;
	private @Nullable MemSize sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
	private @Nullable MemSize receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
	private @Nullable Duration implReadTimeout = DEFAULT_IMPL_READ_TIMEOUT;
	private @Nullable Duration implWriteTimeout = DEFAULT_IMPL_WRITE_TIMEOUT;
	private @Nullable MemSize implReadBufferSize = DEFAULT_IMPL_READ_BUFFER_SIZE;
	private @Nullable Duration lingerTimeout = DEFAULT_LINGER_TIMEOUT;

	private SocketSettings() {
	}

	/**
	 * Creates a default socket settings with socket option {@code TCP_NODELAY} enabled.
	 * Enabling this option turns off Nagle's algorithm.
	 * Note, that by default, operating system has {@code TCP_NODELAY} option disabled.
	 *
	 * @return default socket settings
	 */
	public static SocketSettings create() {
		return builder().build();
	}

	public static SocketSettings defaultInstance() {
		return DEFAULT_INSTANCE;
	}

	/**
	 * Creates a builder for default socket settings with socket option {@code TCP_NODELAY} enabled.
	 * Enabling this option turns off Nagle's algorithm.
	 * Note, that by default, operating system has {@code TCP_NODELAY} option disabled.
	 *
	 * @return default socket settings builder
	 */
	public static Builder builder() {
		return new SocketSettings().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, SocketSettings> {
		private Builder() {}

		public Builder withSendBufferSize(@Nullable MemSize sendBufferSize) {
			checkNotBuilt(this);
			SocketSettings.this.sendBufferSize = sendBufferSize;
			return this;
		}

		public Builder withReceiveBufferSize(@Nullable MemSize receiveBufferSize) {
			checkNotBuilt(this);
			SocketSettings.this.receiveBufferSize = receiveBufferSize;
			return this;
		}

		public Builder withKeepAlive(@Nullable Boolean keepAlive) {
			checkNotBuilt(this);
			SocketSettings.this.keepAlive = keepAlive;
			return this;
		}

		public Builder withReuseAddress(@Nullable Boolean reuseAddress) {
			checkNotBuilt(this);
			SocketSettings.this.reuseAddress = reuseAddress;
			return this;
		}

		public Builder withTcpNoDelay(@Nullable Boolean tcpNoDelay) {
			checkNotBuilt(this);
			SocketSettings.this.tcpNoDelay = tcpNoDelay;
			return this;
		}

		public Builder withImplReadTimeout(@Nullable Duration implReadTimeout) {
			checkNotBuilt(this);
			SocketSettings.this.implReadTimeout = implReadTimeout;
			return this;
		}

		public Builder withImplWriteTimeout(@Nullable Duration implWriteTimeout) {
			checkNotBuilt(this);
			SocketSettings.this.implWriteTimeout = implWriteTimeout;
			return this;
		}

		public Builder withImplReadBufferSize(@Nullable MemSize implReadBufferSize) {
			checkNotBuilt(this);
			SocketSettings.this.implReadBufferSize = implReadBufferSize;
			return this;
		}

		public Builder withLingerTimeout(@Nullable Duration lingerTimeout) {
			checkNotBuilt(this);
			SocketSettings.this.lingerTimeout = lingerTimeout;
			return this;
		}

		@Override
		protected SocketSettings doBuild() {
			return SocketSettings.this;
		}
	}

	public void applySettings(SocketChannel channel) throws IOException {
		if (sendBufferSize != null) {
			channel.setOption(SO_SNDBUF, sendBufferSize.toInt());
		}
		if (receiveBufferSize != null) {
			channel.setOption(SO_RCVBUF, receiveBufferSize.toInt());
		}
		if (keepAlive != null) {
			channel.setOption(SO_KEEPALIVE, keepAlive);
		}
		if (reuseAddress != null) {
			channel.setOption(SO_REUSEADDR, reuseAddress);
		}
		if (tcpNoDelay != null) {
			channel.setOption(TCP_NODELAY, tcpNoDelay);
		}
		if (lingerTimeout != null) {
			channel.setOption(SO_LINGER, (int) lingerTimeout.getSeconds());
		}
	}

	public @Nullable MemSize getSendBufferSize() {
		return sendBufferSize;
	}

	public @Nullable MemSize getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public @Nullable Boolean getKeepAlive() {
		return keepAlive;
	}

	public @Nullable Boolean getReuseAddress() {
		return reuseAddress;
	}

	public @Nullable Boolean getTcpNoDelay() {
		return tcpNoDelay;
	}

	public @Nullable Duration getImplReadTimeout() {
		return implReadTimeout;
	}

	public @Nullable Duration getImplWriteTimeout() {
		return implWriteTimeout;
	}

	public @Nullable MemSize getImplReadBufferSize() {
		return implReadBufferSize;
	}

	public @Nullable Duration getLingerTimeout() {
		return lingerTimeout;
	}
}
