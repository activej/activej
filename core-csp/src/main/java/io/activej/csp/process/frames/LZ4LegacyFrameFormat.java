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

import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.parse.ParseException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import static io.activej.common.Checks.checkArgument;

@Deprecated
public final class LZ4LegacyFrameFormat implements FrameFormat {
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

	private int compressionLevel;
	private boolean ignoreMissingEndOfStreamBlock;
	private boolean safeDecompressor;

	private LZ4LegacyFrameFormat(LZ4Factory factory, XXHashFactory hashFactory) {
		this.lz4Factory = factory;
		this.hashFactory = hashFactory;
	}

	public static LZ4LegacyFrameFormat create() {
		return new LZ4LegacyFrameFormat(LZ4Factory.fastestInstance(), XXHashFactory.fastestInstance());
	}

	public static LZ4LegacyFrameFormat create(LZ4Factory lz4Factory, XXHashFactory hashFactory) {
		return new LZ4LegacyFrameFormat(lz4Factory, hashFactory);
	}

	public LZ4LegacyFrameFormat withHighCompression() {
		this.compressionLevel = -1;
		return this;
	}

	public LZ4LegacyFrameFormat withCompressionLevel(int compressionLevel) {
		checkArgument(compressionLevel >= -1);
		this.compressionLevel = compressionLevel;
		return this;
	}

	public LZ4LegacyFrameFormat withIgnoreMissingEndOfStream(boolean ignore) {
		this.ignoreMissingEndOfStreamBlock = ignore;
		return this;
	}

	public LZ4LegacyFrameFormat withSafeDecompressor(boolean safeDecompressor) {
		this.safeDecompressor = safeDecompressor;
		return this;
	}

	@Override
	public BlockEncoder createEncoder() {
		LZ4Compressor compressor = compressionLevel == 0 ?
				lz4Factory.fastCompressor() :
				compressionLevel == -1 ?
						lz4Factory.highCompressor() :
						lz4Factory.highCompressor(compressionLevel);
		return new LZ4LegacyBlockEncoder(compressor, hashFactory.newStreamingHash32(DEFAULT_SEED));
	}

	@Override
	public BlockDecoder createDecoder() {
		StreamingXXHash32 checksum = hashFactory.newStreamingHash32(DEFAULT_SEED);
		return safeDecompressor ?
				new LZ4LegacyBlockDecoder(checksum, ignoreMissingEndOfStreamBlock) {
					final LZ4SafeDecompressor decompressor = lz4Factory.safeDecompressor();

					@Override
					protected void decompress(ByteBuf outputBuf, byte[] bytes, int off) {
						decompressor.decompress(bytes, off, this.compressedLen, outputBuf.array(), 0, originalLen);
					}
				} :
				new LZ4LegacyBlockDecoder(checksum, ignoreMissingEndOfStreamBlock) {
					final LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();

					@Override
					protected void decompress(ByteBuf outputBuf, byte[] bytes, int off) throws ParseException {
						int compressedLen = decompressor.decompress(bytes, off, outputBuf.array(), 0, originalLen);
						if (compressedLen != this.compressedLen) {
							throw STREAM_IS_CORRUPTED;
						}
					}
				};
	}
}
