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

package io.activej.csp.process.frames;

import io.activej.common.initializer.WithInitializer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.util.zip.Checksum;

import static io.activej.common.Checks.checkArgument;

@Deprecated
public final class FrameFormat_LZ4Legacy implements FrameFormat, WithInitializer<FrameFormat_LZ4Legacy> {
	static final byte[] MAGIC = {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
	static final byte[] LAST_BYTES;
	static final byte[] MAGIC_AND_LAST_BYTES;
	static final int MAGIC_LENGTH = MAGIC.length;

	static final int COMPRESSION_LEVEL_BASE = 10;

	static final int COMPRESSION_METHOD_RAW = 0x10;
	static final int COMPRESSION_METHOD_LZ4 = 0x20;

	static {
		LAST_BYTES = new byte[13];
		LAST_BYTES[0] = COMPRESSION_METHOD_RAW;

		MAGIC_AND_LAST_BYTES = new byte[MAGIC.length + LAST_BYTES.length];
		System.arraycopy(MAGIC, 0, MAGIC_AND_LAST_BYTES, 0, MAGIC.length);
		MAGIC_AND_LAST_BYTES[MAGIC.length] = COMPRESSION_METHOD_RAW;
	}

	static final int HEADER_LENGTH =
			MAGIC.length    // magic bytes
					+ 1     // token
					+ 4     // compressed length
					+ 4     // decompressed length
					+ 4;    // checksum

	static final int DEFAULT_SEED = 0x9747b28c;

	private final LZ4Factory lz4Factory;
	private final XXHashFactory hashFactory;
	private boolean legacyChecksum;

	private int compressionLevel;

	private boolean ignoreMissingEndOfStreamBlock;

	private FrameFormat_LZ4Legacy(LZ4Factory factory, XXHashFactory hashFactory) {
		this.lz4Factory = factory;
		this.hashFactory = hashFactory;
	}

	public static FrameFormat_LZ4Legacy create() {
		return new FrameFormat_LZ4Legacy(LZ4Factory.fastestInstance(), XXHashFactory.fastestInstance());
	}

	public static FrameFormat_LZ4Legacy create(LZ4Factory lz4Factory, XXHashFactory hashFactory) {
		return new FrameFormat_LZ4Legacy(lz4Factory, hashFactory);
	}

	public FrameFormat_LZ4Legacy withHighCompression() {
		this.compressionLevel = -1;
		return this;
	}

	public FrameFormat_LZ4Legacy withCompressionLevel(int compressionLevel) {
		checkArgument(compressionLevel >= -1);
		this.compressionLevel = compressionLevel;
		return this;
	}

	public FrameFormat_LZ4Legacy withIgnoreMissingEndOfStream(boolean ignore) {
		this.ignoreMissingEndOfStreamBlock = ignore;
		return this;
	}

	/**
	 * Whether streaming hash will be used as a {@link Checksum}, same as in LZ4 library stream encoder/decoder
	 * <p>
	 * Useful for interoperation with {@link net.jpountz.lz4.LZ4BlockOutputStream} and {@link net.jpountz.lz4.LZ4BlockInputStream}
	 */
	@Deprecated
	public FrameFormat_LZ4Legacy withLegacyChecksum(boolean legacyChecksum) {
		this.legacyChecksum = legacyChecksum;
		return this;
	}

	@Override
	public BlockEncoder createEncoder() {
		LZ4Compressor compressor = compressionLevel == 0 ?
				lz4Factory.fastCompressor() :
				compressionLevel == -1 ?
						lz4Factory.highCompressor() :
						lz4Factory.highCompressor(compressionLevel);
		StreamingXXHash32 hash = hashFactory.newStreamingHash32(DEFAULT_SEED);
		Checksum checksum = legacyChecksum ? hash.asChecksum() : toSimpleChecksum(hash);
		return new BlockEncoder_LZ4Legacy(compressor, checksum);
	}

	@Override
	public BlockDecoder createDecoder() {
		StreamingXXHash32 hash = hashFactory.newStreamingHash32(DEFAULT_SEED);
		Checksum checksum = legacyChecksum ? hash.asChecksum() : toSimpleChecksum(hash);
		return new BlockDecoder_LZ4Legacy(lz4Factory.fastDecompressor(), checksum, ignoreMissingEndOfStreamBlock);
	}

	private static Checksum toSimpleChecksum(StreamingXXHash32 hash) {
		return new Checksum() {
			@Override
			public void update(int b) {
				hash.update(new byte[]{(byte) b}, 0, 1);
			}

			@Override
			public void update(byte[] b, int off, int len) {
				hash.update(b, off, len);
			}

			@Override
			public long getValue() {
				return hash.getValue();
			}

			@Override
			public void reset() {
				hash.reset();
			}
		};
	}
}
