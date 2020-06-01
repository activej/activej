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

package io.activej.csp.process;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.csp.*;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ValueStats;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

import static io.activej.common.Preconditions.checkArgument;
import static java.lang.Math.max;

public final class ChannelLZ4Compressor extends AbstractCommunicatingProcess
		implements WithChannelTransformer<ChannelLZ4Compressor, ByteBuf, ByteBuf> {
	public static final byte[] MAGIC = {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
	public static final int MAGIC_LENGTH = MAGIC.length;

	public static final int HEADER_LENGTH =
			MAGIC_LENGTH    // magic bytes
					+ 1     // token
					+ 4     // compressed length
					+ 4     // decompressed length
					+ 4;    // checksum

	static final int COMPRESSION_LEVEL_BASE = 10;

	static final int COMPRESSION_METHOD_RAW = 0x10;
	static final int COMPRESSION_METHOD_LZ4 = 0x20;

	static final int DEFAULT_SEED = 0x9747b28c;

	private static final int MIN_BLOCK_SIZE = 64;

	private final LZ4Compressor compressor;
	private final StreamingXXHash32 checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED);

	private ChannelSupplier<ByteBuf> input;
	private ChannelConsumer<ByteBuf> output;

	@Nullable
	private Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onBuf(ByteBuf in, ByteBuf out);
	}

	public abstract static class ForwardingInspector implements Inspector {
		protected final @Nullable Inspector next;

		public ForwardingInspector(@Nullable Inspector next) {this.next = next;}

		@Override
		public void onBuf(ByteBuf in, ByteBuf out) {
			if (next != null) next.onBuf(in, out);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Inspector> @Nullable T lookup(Class<T> type) {
			return type.isAssignableFrom(this.getClass()) ? (T) this : next != null ? next.lookup(type) : null;
		}
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		public static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final ValueStats bytesIn = ValueStats.create(SMOOTHING_WINDOW);
		private final ValueStats bytesOut = ValueStats.create(SMOOTHING_WINDOW);

		@Override
		public void onBuf(ByteBuf in, ByteBuf out) {
			bytesIn.recordValue(in.readRemaining());
			bytesOut.recordValue(out.readRemaining());
		}

		@JmxAttribute
		public ValueStats getBytesIn() {
			return bytesIn;
		}

		@JmxAttribute
		public ValueStats getBytesOut() {
			return bytesOut;
		}
	}

	// region creators
	private ChannelLZ4Compressor(LZ4Compressor compressor) {
		this.compressor = compressor;
	}

	public static ChannelLZ4Compressor create(LZ4Compressor compressor) {
		return new ChannelLZ4Compressor(compressor);
	}

	public static ChannelLZ4Compressor create(int compressionLevel) {
		return compressionLevel == 0 ? createFastCompressor() : createHighCompressor(compressionLevel);
	}

	public static ChannelLZ4Compressor createFastCompressor() {
		return new ChannelLZ4Compressor(LZ4Factory.fastestInstance().fastCompressor());
	}

	public static ChannelLZ4Compressor createHighCompressor() {
		return new ChannelLZ4Compressor(LZ4Factory.fastestInstance().highCompressor());
	}

	public static ChannelLZ4Compressor createHighCompressor(int compressionLevel) {
		return new ChannelLZ4Compressor(LZ4Factory.fastestInstance().highCompressor(compressionLevel));
	}

	public ChannelLZ4Compressor withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	//check input for clarity
	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			this.input = sanitize(input);
			//noinspection ConstantConditions
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (buf != null) {
						ByteBuf outputBuf = compressBlock(compressor, checksum, buf.array(), buf.head(), buf.readRemaining());
						if (inspector != null) inspector.onBuf(buf, outputBuf);
						buf.recycle();
						output.accept(outputBuf)
								.whenResult(this::doProcess);
					} else {
						output.acceptAll(createEndOfStreamBlock(), null)
								.whenResult(this::completeProcess);
					}
				});
	}

	@Override
	protected void doClose(Throwable e) {
		input.closeEx(e);
		output.closeEx(e);
	}

	// endregion

	private static int compressionLevel(int blockSize) {
		int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
		checkArgument((1 << compressionLevel) >= blockSize);
		checkArgument(blockSize * 2 > (1 << compressionLevel));
		compressionLevel = max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
		checkArgument(compressionLevel <= 0x0F);
		return compressionLevel;
	}

	private static void writeIntLE(int i, byte[] buf, int off) {
		buf[off++] = (byte) i;
		buf[off++] = (byte) (i >>> 8);
		buf[off++] = (byte) (i >>> 16);
		buf[off] = (byte) (i >>> 24);
	}

	private static ByteBuf compressBlock(LZ4Compressor compressor, StreamingXXHash32 checksum, byte[] bytes, int off, int len) {
		checkArgument(len != 0);

		int compressionLevel = compressionLevel(max(len, MIN_BLOCK_SIZE));

		int outputBufMaxSize = HEADER_LENGTH + ((compressor == null) ? len : compressor.maxCompressedLength(len));
		ByteBuf outputBuf = ByteBufPool.allocate(outputBufMaxSize);
		outputBuf.put(MAGIC);

		byte[] outputBytes = outputBuf.array();

		checksum.reset();
		checksum.update(bytes, off, len);
		int check = checksum.getValue();

		int compressedLength = len;
		if (compressor != null) {
			compressedLength = compressor.compress(bytes, off, len, outputBytes, HEADER_LENGTH);
		}

		int compressMethod;
		if (compressor == null || compressedLength >= len) {
			compressMethod = COMPRESSION_METHOD_RAW;
			compressedLength = len;
			System.arraycopy(bytes, off, outputBytes, HEADER_LENGTH, len);
		} else {
			compressMethod = COMPRESSION_METHOD_LZ4;
		}

		outputBytes[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
		writeIntLE(compressedLength, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(len, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(check, outputBytes, MAGIC_LENGTH + 9);

		outputBuf.tail(HEADER_LENGTH + compressedLength);

		return outputBuf;
	}

	private static ByteBuf createEndOfStreamBlock() {
		int compressionLevel = compressionLevel(MIN_BLOCK_SIZE);

		ByteBuf outputBuf = ByteBufPool.allocate(HEADER_LENGTH);
		byte[] outputBytes = outputBuf.array();
		System.arraycopy(MAGIC, 0, outputBytes, 0, MAGIC_LENGTH);

		outputBytes[MAGIC_LENGTH] = (byte) (COMPRESSION_METHOD_RAW | compressionLevel);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 9);

		outputBuf.tail(HEADER_LENGTH);
		return outputBuf;
	}

}
