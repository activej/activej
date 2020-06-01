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

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

import static io.activej.common.Preconditions.checkState;
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

	private final int backlog;
	private final int receiveBufferSize;
	private final byte reuseAddress;

	// region builders
	private ServerSocketSettings(int backlog, int receiveBufferSize, byte reuseAddress) {
		this.backlog = backlog;
		this.receiveBufferSize = receiveBufferSize;
		this.reuseAddress = reuseAddress;
	}

	public static ServerSocketSettings create(int backlog) {
		return new ServerSocketSettings(backlog, 0, DEF_BOOL);
	}

	public ServerSocketSettings withBacklog(int backlog) {
		return new ServerSocketSettings(backlog, receiveBufferSize, reuseAddress);
	}

	public ServerSocketSettings withReceiveBufferSize(@NotNull MemSize receiveBufferSize) {
		return new ServerSocketSettings(backlog, receiveBufferSize.toInt(), reuseAddress);
	}

	public ServerSocketSettings withReuseAddress(boolean reuseAddress) {
		return new ServerSocketSettings(backlog, receiveBufferSize, reuseAddress ? TRUE : FALSE);
	}
	// endregion

	public void applySettings(@NotNull ServerSocketChannel channel) throws IOException {
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

	@NotNull
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
}
