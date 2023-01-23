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

import java.io.IOException;
import java.nio.channels.DatagramChannel;

import static io.activej.common.Checks.checkState;
import static java.net.StandardSocketOptions.*;

/**
 * This class is used to change settings for datagram socket. It will be applied when creating a new socket
 */
public final class DatagramSocketSettings {
	private static final byte DEF_BOOL = -1;
	private static final byte TRUE = 1;
	private static final byte FALSE = 0;

	private int receiveBufferSize = 0;
	private byte reuseAddress = DEF_BOOL;
	private int sendBufferSize = 0;
	private byte broadcast = DEF_BOOL;

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

		public Builder withReceiveBufferSize(MemSize receiveBufferSize) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.receiveBufferSize = receiveBufferSize.toInt();
			return this;
		}

		public Builder withSendBufferSize(MemSize sendBufferSize) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.sendBufferSize = sendBufferSize.toInt();
			return this;
		}

		public Builder withReuseAddress(boolean reuseAddress) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.reuseAddress = reuseAddress ? TRUE : FALSE;
			return this;
		}

		public Builder withBroadcast(boolean broadcast) {
			checkNotBuilt(this);
			DatagramSocketSettings.this.broadcast = broadcast ? TRUE : FALSE;
			return this;
		}

		@Override
		protected DatagramSocketSettings doBuild() {
			return DatagramSocketSettings.this;
		}
	}

	public void applySettings(DatagramChannel channel) throws IOException {
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

	public boolean hasBroadcast() {
		return broadcast != DEF_BOOL;
	}

	public boolean getBroadcast() {
		checkState(hasBroadcast(), "No 'broadcast' setting is present");
		return broadcast != FALSE;
	}
}
