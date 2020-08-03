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

package io.activej.eventloop.net;

import io.activej.common.MemSize;
import org.jetbrains.annotations.NotNull;
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

	private final byte keepAlive;
	private final byte reuseAddress;
	private final byte tcpNoDelay;

	private final int sendBufferSize;
	private final int receiveBufferSize;
	private final int implReadTimeout;
	private final int implWriteTimeout;
	private final int implReadBufferSize;
	private final int lingerTimeout;

	// region builders
	private SocketSettings(int sendBufferSize, int receiveBufferSize, byte keepAlive, byte reuseAddress, byte tcpNoDelay, int implReadTimeout, int implWriteTimeout, int implReadBufferSize, int lingerTimeout) {
		this.sendBufferSize = sendBufferSize;
		this.receiveBufferSize = receiveBufferSize;
		this.keepAlive = keepAlive;
		this.reuseAddress = reuseAddress;
		this.tcpNoDelay = tcpNoDelay;
		this.implReadTimeout = implReadTimeout;
		this.implWriteTimeout = implWriteTimeout;
		this.implReadBufferSize = implReadBufferSize;
		this.lingerTimeout = lingerTimeout;
	}

	public static SocketSettings create() {
		return new SocketSettings(0, 0, DEF_BOOL, DEF_BOOL, DEF_BOOL, 0, 0, 0, -1);
	}

	/**
	 * Creates a default socket settings with socket option {@code TCP_NODELAY} enabled.
	 * Enabling this option turns off Nagle's algorithm.
	 * Note, that by default, operating system has {@code TCP_NODELAY} option disabled.
	 *
	 * @return default socket settings
	 */
	public static SocketSettings createDefault() {
		return new SocketSettings(0, 0, DEF_BOOL, DEF_BOOL, TRUE, 0, 0, 0, -1);
	}

	public SocketSettings withSendBufferSize(@NotNull MemSize sendBufferSize) {
		return new SocketSettings(sendBufferSize.toInt(), receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadBufferSize, lingerTimeout);
	}

	public SocketSettings withReceiveBufferSize(@NotNull MemSize receiveBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize.toInt(), keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadBufferSize, lingerTimeout);
	}

	public SocketSettings withKeepAlive(boolean keepAlive) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive ? TRUE : FALSE, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadBufferSize, lingerTimeout);
	}

	public SocketSettings withReuseAddress(boolean reuseAddress) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress ? TRUE : FALSE, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadBufferSize, lingerTimeout);
	}

	public SocketSettings withTcpNoDelay(boolean tcpNoDelay) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay ? TRUE : FALSE, implReadTimeout, implWriteTimeout, implReadBufferSize, lingerTimeout);
	}

	public SocketSettings withImplReadTimeout(@NotNull Duration implReadTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, (int) implReadTimeout.toMillis(), implWriteTimeout, implReadBufferSize, lingerTimeout);
	}

	public SocketSettings withImplWriteTimeout(@NotNull Duration implWriteTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, (int) implWriteTimeout.toMillis(), implReadBufferSize, lingerTimeout);
	}

	public SocketSettings withImplReadBufferSize(@NotNull MemSize implReadBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadBufferSize.toInt(), lingerTimeout);
	}

	public SocketSettings withLingerTimeout(Duration lingerTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadBufferSize, (int) (lingerTimeout.toMillis() / 1000));
	}

	// endregion

	public void applySettings(@NotNull SocketChannel channel) throws IOException {
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
		if (lingerTimeout != -1){
			channel.setOption(SO_LINGER, lingerTimeout);
		}
	}

	public boolean hasSendBufferSize() {
		return sendBufferSize != 0;
	}

	@NotNull
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

	@NotNull
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

	@NotNull
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

	@NotNull
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

	@NotNull
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

	@Nullable
	public Duration getLingerTimeout() {
		return lingerTimeout == -1 ? null : Duration.ofSeconds(lingerTimeout);
	}

	public int getLingerTimeoutSeconds() {
		checkState(hasLingerTimeout(), "No 'linger timeout' setting is present");
		return implReadBufferSize;
	}
}
