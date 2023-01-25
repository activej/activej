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
import io.activej.common.builder.AbstractBuilder;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.time.Duration;

import static io.activej.common.Checks.checkState;
import static java.net.StandardSocketOptions.*;

/**
 * This class is used to hold settings for a socket. Settings  will be applied when creating new socket
 */
public final class SocketSettings {
	private static final byte DEF_BOOL = -1;
	private static final byte TRUE = 1;
	private static final byte FALSE = 0;

	private byte keepAlive = DEF_BOOL;
	private byte reuseAddress = DEF_BOOL;
	private byte tcpNoDelay = DEF_BOOL;

	private int sendBufferSize = 0;
	private int receiveBufferSize = 0;
	private int implReadTimeout = 0;
	private int implWriteTimeout = 0;
	private int implReadBufferSize = 0;
	private int lingerTimeout = -1;

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

	public static SocketSettings createDefault() {
		return builder().withTcpNoDelay(true).build();
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

		public Builder withSendBufferSize(MemSize sendBufferSize) {
			checkNotBuilt(this);
			SocketSettings.this.sendBufferSize = sendBufferSize.toInt();
			return this;
		}

		public Builder withReceiveBufferSize(MemSize receiveBufferSize) {
			checkNotBuilt(this);
			SocketSettings.this.receiveBufferSize = receiveBufferSize.toInt();
			return this;
		}

		public Builder withKeepAlive(boolean keepAlive) {
			checkNotBuilt(this);
			SocketSettings.this.keepAlive = keepAlive ? TRUE : FALSE;
			return this;
		}

		public Builder withReuseAddress(boolean reuseAddress) {
			checkNotBuilt(this);
			SocketSettings.this.reuseAddress = reuseAddress ? TRUE : FALSE;
			return this;
		}

		public Builder withTcpNoDelay(boolean tcpNoDelay) {
			checkNotBuilt(this);
			SocketSettings.this.tcpNoDelay = tcpNoDelay ? TRUE : FALSE;
			return this;
		}

		public Builder withImplReadTimeout(Duration implReadTimeout) {
			checkNotBuilt(this);
			SocketSettings.this.implReadTimeout = (int) implReadTimeout.toMillis();
			return this;
		}

		public Builder withImplWriteTimeout(Duration implWriteTimeout) {
			checkNotBuilt(this);
			SocketSettings.this.implWriteTimeout = (int) implWriteTimeout.toMillis();
			return this;
		}

		public Builder withImplReadBufferSize(MemSize implReadBufferSize) {
			checkNotBuilt(this);
			SocketSettings.this.implReadBufferSize = implReadBufferSize.toInt();
			return this;
		}

		public Builder withLingerTimeout(Duration lingerTimeout) {
			checkNotBuilt(this);
			SocketSettings.this.lingerTimeout = (int) (lingerTimeout.toMillis() / 1000);
			return this;
		}

		@Override
		protected SocketSettings doBuild() {
			return SocketSettings.this;
		}
	}

	public void applySettings(SocketChannel channel) throws IOException {
		if (sendBufferSize != 0) {
			channel.setOption(SO_SNDBUF, sendBufferSize);
		}
		if (receiveBufferSize != 0) {
			channel.setOption(SO_RCVBUF, receiveBufferSize);
		}
		if (keepAlive != DEF_BOOL) {
			channel.setOption(SO_KEEPALIVE, keepAlive != FALSE);
		}
		if (reuseAddress != DEF_BOOL) {
			channel.setOption(SO_REUSEADDR, reuseAddress != FALSE);
		}
		if (tcpNoDelay != DEF_BOOL) {
			channel.setOption(TCP_NODELAY, tcpNoDelay != FALSE);
		}
		if (lingerTimeout != -1) {
			channel.setOption(SO_LINGER, lingerTimeout);
		}
	}

	public boolean hasSendBufferSize() {
		return sendBufferSize != 0;
	}

	public MemSize getSendBufferSize() {
		return MemSize.of(getSendBufferSizeBytes());
	}

	public int getSendBufferSizeBytes() {
		checkState(hasSendBufferSize(), "No 'send buffer size' setting is present");
		return sendBufferSize;
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

	public boolean hasKeepAlive() {
		return keepAlive != DEF_BOOL;
	}

	public boolean getKeepAlive() {
		checkState(hasKeepAlive(), "No 'keep alive' setting is present");
		return keepAlive != FALSE;
	}

	public boolean hasReuseAddress() {
		return reuseAddress != DEF_BOOL;
	}

	public boolean getReuseAddress() {
		checkState(hasReuseAddress(), "No 'reuse address' setting is present");
		return reuseAddress != FALSE;
	}

	public boolean hasTcpNoDelay() {
		return tcpNoDelay != DEF_BOOL;
	}

	public boolean getTcpNoDelay() {
		checkState(hasTcpNoDelay(), "No 'TCP no delay' setting is present");
		return tcpNoDelay != FALSE;
	}

	public boolean hasImplReadTimeout() {
		return implReadTimeout != 0;
	}

	public Duration getImplReadTimeout() {
		return Duration.ofMillis(getImplReadTimeoutMillis());
	}

	public long getImplReadTimeoutMillis() {
		checkState(hasImplReadTimeout(), "No 'implicit read timeout' setting is present");
		return implReadTimeout;
	}

	public boolean hasImplWriteTimeout() {
		return implWriteTimeout != 0;
	}

	public Duration getImplWriteTimeout() {
		return Duration.ofMillis(getImplWriteTimeoutMillis());
	}

	public long getImplWriteTimeoutMillis() {
		checkState(hasImplWriteTimeout(), "No 'implicit write timeout' setting is present");
		return implWriteTimeout;
	}

	public boolean hasReadBufferSize() {
		return implReadBufferSize != 0;
	}

	public MemSize getImplReadBufferSize() {
		return MemSize.of(getImplReadBufferSizeBytes());
	}

	public int getImplReadBufferSizeBytes() {
		checkState(hasReadBufferSize(), "No 'read buffer size' setting is present");
		return implReadBufferSize;
	}

	public boolean hasLingerTimeout() {
		return lingerTimeout != -1;
	}

	public @Nullable Duration getLingerTimeout() {
		return lingerTimeout == -1 ? null : Duration.ofSeconds(lingerTimeout);
	}

	public int getLingerTimeoutSeconds() {
		checkState(hasLingerTimeout(), "No 'linger timeout' setting is present");
		return implReadBufferSize;
	}
}
