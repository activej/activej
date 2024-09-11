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

package io.activej.http.stream;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.csp.supplier.ChannelSupplier;

import java.util.zip.CRC32;
import java.util.zip.Deflater;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * This is a binary channel transformer, that converts channels of {@link ByteBuf ByteBufs}
 * compressing the data using the DEFLATE algorithm with standard implementation from the java.util.zip package.
 * <p>
 * It is used in HTTP when {@link io.activej.http.HttpMessage.Builder#withBodyGzipCompression HttpMessage.Builder#withBodyGzipCompression}
 * method is used.
 */
public final class BufsConsumerGzipDeflater extends AbstractCommunicatingProcess
	implements WithChannelTransformer<BufsConsumerGzipDeflater, ByteBuf, ByteBuf> {
	public static final int DEFAULT_MAX_BUF_SIZE = ApplicationSettings.getInt(BufsConsumerGzipDeflater.class, "maxBufferSize", 16384);
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, (byte) 0xff};
	private static final int GZIP_FOOTER_SIZE = 8;

	private final CRC32 crc32 = new CRC32();

	private Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
	private int maxBufSize = DEFAULT_MAX_BUF_SIZE;
	private ChannelSupplier<ByteBuf> input;
	private ChannelConsumer<ByteBuf> output;

	private BufsConsumerGzipDeflater() {}

	public static BufsConsumerGzipDeflater create() {
		return builder().build();
	}

	public static Builder builder() {
		return new BufsConsumerGzipDeflater().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, BufsConsumerGzipDeflater> {
		private Builder() {}

		public Builder withDeflater(Deflater deflater) {
			checkNotBuilt(this);
			BufsConsumerGzipDeflater.this.deflater = deflater;
			return this;
		}

		public Builder withMaxBufSize(MemSize maxBufSize) {
			checkNotBuilt(this);
			checkArgument(maxBufSize.compareTo(MemSize.ZERO) > 0, "Cannot use buf size that is less than 0");
			BufsConsumerGzipDeflater.this.maxBufSize = maxBufSize.toInt();
			return this;
		}

		@Override
		protected BufsConsumerGzipDeflater doBuild() {
			return BufsConsumerGzipDeflater.this;
		}
	}

	@SuppressWarnings("ConstantConditions") //check input for clarity
	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			checkInReactorThread(this);
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkInReactorThread(this);
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		writeHeader();
	}

	private void writeHeader() {
		output.accept(ByteBuf.wrapForReading(GZIP_HEADER))
			.whenResult(this::writeBody);
	}

	private void writeBody() {
		input.streamTo(ChannelConsumers.ofAsyncConsumer(buf -> {
				crc32.update(buf.array(), buf.head(), buf.readRemaining());
				deflater.setInput(buf.array(), buf.head(), buf.readRemaining());
				ByteBufs bufs = deflate();
				buf.recycle();
				return output.acceptAll(bufs.asIterator());
			}))
			.whenResult(this::writeFooter);
	}

	private void writeFooter() {
		deflater.finish();
		ByteBufs bufs = deflate();
		ByteBuf footer = ByteBufPool.allocate(GZIP_FOOTER_SIZE);
		footer.writeInt(Integer.reverseBytes((int) crc32.getValue()));
		footer.writeInt(Integer.reverseBytes(deflater.getTotalIn()));
		bufs.add(footer);
		output.acceptAll(bufs.asIterator())
			.then(output::acceptEndOfStream)
			.whenResult(this::completeProcess);
	}

	private ByteBufs deflate() {
		ByteBufs bufs = new ByteBufs();
		while (true) {
			ByteBuf out = ByteBufPool.allocate(maxBufSize);
			int len = deflater.deflate(out.array(), out.tail(), out.writeRemaining());
			if (len > 0) {
				out.tail(len);
				bufs.add(out);
			} else {
				out.recycle();
				return bufs;
			}
		}
	}

	@Override
	protected void doClose(Exception e) {
		deflater.end();
		input.closeEx(e);
		output.closeEx(e);
	}
}
