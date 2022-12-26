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
import io.activej.common.initializer.WithInitializer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.DatagramChannel;

import static io.activej.common.Checks.checkState;
import static java.net.StandardSocketOptions.*;

/**
 * This class used to change settings for socket. It will be applying with creating new socket
 */
public final class DatagramSocketSettings implements WithInitializer<DatagramSocketSettings> {
	private static final byte DEF_BOOL = -1;
	private static final byte TRUE = 1;
	private static final byte FALSE = 0;

	private final int receiveBufferSize;
	private final byte reuseAddress;
	private final int sendBufferSize;
	private final byte broadcast;

	// region builders
	private DatagramSocketSettings(int receiveBufferSize, int sendBufferSize, byte reuseAddress, byte broadcast) {
		this.receiveBufferSize = receiveBufferSize;
		this.reuseAddress = reuseAddress;
		this.sendBufferSize = sendBufferSize;
		this.broadcast = broadcast;
	}

	public static DatagramSocketSettings create() {
		return new DatagramSocketSettings(0, 0, DEF_BOOL, DEF_BOOL);
	}

	public DatagramSocketSettings withReceiveBufferSize(@NotNull MemSize receiveBufferSize) {
		return new DatagramSocketSettings(receiveBufferSize.toInt(), sendBufferSize, reuseAddress, broadcast);
	}

	public DatagramSocketSettings withSendBufferSize(@NotNull MemSize sendBufferSize) {
		return new DatagramSocketSettings(receiveBufferSize, sendBufferSize.toInt(), reuseAddress, broadcast);
	}

	public DatagramSocketSettings withReuseAddress(boolean reuseAddress) {
		return new DatagramSocketSettings(receiveBufferSize, sendBufferSize, reuseAddress ? TRUE : FALSE, broadcast);
	}

	public DatagramSocketSettings withBroadcast(boolean broadcast) {
		return new DatagramSocketSettings(receiveBufferSize, sendBufferSize, reuseAddress, broadcast ? TRUE : FALSE);
	}
	// endregion

	public void applySettings(@NotNull DatagramChannel channel) throws IOException {
		if (receiveBufferSize != 0) {
			channel.setOption(SO_RCVBUF, receiveBufferSize);
		}
		if (sendBufferSize != 0) {
			channel.setOption(SO_SNDBUF, sendBufferSize);
		}
		if (reuseAddress != DEF_BOOL) {
			channel.setOption(SO_REUSEADDR, reuseAddress != FALSE);
		}
		if (broadcast != DEF_BOOL) {
			channel.setOption(SO_BROADCAST, broadcast != FALSE);
		}
	}

	public boolean hasReceiveBufferSize() {
		return receiveBufferSize != 0;
	}

	public @NotNull MemSize getReceiveBufferSize() {
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

	public boolean hasSendBufferSize() {
		return sendBufferSize != 0;
	}

	public @NotNull MemSize getSendBufferSize() {
		return MemSize.of(getSendBufferSizeBytes());
	}

	public int getSendBufferSizeBytes() {
		checkState(hasSendBufferSize(), "No 'send buffer size' setting is present");
		return sendBufferSize;
	}

	public boolean hasBroadcast() {
		return broadcast != DEF_BOOL;
	}

	public boolean getBroadcast() {
		checkState(hasBroadcast(), "No 'broadcast' setting is present");
		return broadcast != FALSE;
	}
}
