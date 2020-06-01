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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.csp.*;
import io.activej.csp.dsl.WithChannelTransformer;
import org.jetbrains.annotations.NotNull;

import java.util.zip.CRC32;
import java.util.zip.Deflater;

import static io.activej.common.Preconditions.checkArgument;
import static io.activej.common.Preconditions.checkState;

/**
 * This is a binary channel transformer, that converts channels of {@link ByteBuf ByteBufs}
 * compressing the data using the DEFLATE algorithm with standard implementation from the java.util.zip package.
 * <p>
 * It is used in HTTP when {@link io.activej.http.HttpMessage#setBodyGzipCompression HttpMessage#setBodyGzipCompression}
 * method is used.
 */
public final class BufsConsumerGzipDeflater extends AbstractCommunicatingProcess
		implements WithChannelTransformer<BufsConsumerGzipDeflater, ByteBuf, ByteBuf> {
	public static final int DEFAULT_MAX_BUF_SIZE = 16384;
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
	private static final int GZIP_FOOTER_SIZE = 8;

	private final CRC32 crc32 = new CRC32();

	private Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
	private int maxBufSize = DEFAULT_MAX_BUF_SIZE;
	private ChannelSupplier<ByteBuf> input;
	private ChannelConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerGzipDeflater() {
	}

	public static BufsConsumerGzipDeflater create() {
		return new BufsConsumerGzipDeflater();
	}

	public BufsConsumerGzipDeflater withDeflater(@NotNull Deflater deflater) {
		this.deflater = deflater;
		return this;
	}

	public BufsConsumerGzipDeflater withMaxBufSize(MemSize maxBufSize) {
		checkArgument(maxBufSize.compareTo(MemSize.ZERO) > 0, "Cannot use buf size that is less than 0");
		this.maxBufSize = maxBufSize.toInt();
		return this;
	}

	@SuppressWarnings("ConstantConditions") //check input for clarity
	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
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
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}
	// endregion

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
		input.streamTo(ChannelConsumer.of(buf -> {
					crc32.update(buf.array(), buf.head(), buf.readRemaining());
					deflater.setInput(buf.array(), buf.head(), buf.readRemaining());
					ByteBufQueue queue = deflate();
					buf.recycle();
					return output.acceptAll(queue.asIterator());
				}))
				.whenResult(this::writeFooter);
	}

	private void writeFooter() {
		deflater.finish();
		ByteBufQueue queue = deflate();
		ByteBuf footer = ByteBufPool.allocate(GZIP_FOOTER_SIZE);
		footer.writeInt(Integer.reverseBytes((int) crc32.getValue()));
		footer.writeInt(Integer.reverseBytes(deflater.getTotalIn()));
		queue.add(footer);
		output.acceptAll(queue.asIterator())
				.then(output::acceptEndOfStream)
				.whenResult(this::completeProcess);
	}

	private ByteBufQueue deflate() {
		ByteBufQueue queue = new ByteBufQueue();
		while (true) {
			ByteBuf out = ByteBufPool.allocate(maxBufSize);
			int len = deflater.deflate(out.array(), out.tail(), out.writeRemaining());
			if (len > 0) {
				out.tail(len);
				queue.add(out);
			} else {
				out.recycle();
				return queue;
			}
		}
	}

	@Override
	protected void doClose(Throwable e) {
		deflater.end();
		input.closeEx(e);
		output.closeEx(e);
	}
}
