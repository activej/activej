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

import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.common.initializer.WithInitializer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import static io.activej.common.Checks.checkArgument;

public final class FrameFormat_LZ4 implements FrameFormat, WithInitializer<FrameFormat_LZ4> {
	public static final MemSize MAX_BLOCK_SIZE = ApplicationSettings.getMemSize(FrameFormat_LZ4.class, "maxBlockSize", MemSize.megabytes(256));

	static final byte[] MAGIC = {'L', 'Z', '4', 1};
	static final byte[] LAST_BLOCK_BYTES = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
	static final byte[] MAGIC_AND_LAST_BLOCK_BYTES;
	static final int MAGIC_LENGTH = MAGIC.length;

	static {
		MAGIC_AND_LAST_BLOCK_BYTES = new byte[MAGIC.length + LAST_BLOCK_BYTES.length];
		System.arraycopy(MAGIC, 0, MAGIC_AND_LAST_BLOCK_BYTES, 0, MAGIC.length);
		System.arraycopy(LAST_BLOCK_BYTES, 0, MAGIC_AND_LAST_BLOCK_BYTES, MAGIC.length, LAST_BLOCK_BYTES.length);
	}

	static final int COMPRESSED_LENGTH_MASK = 0x7fffffff;
	static final byte END_OF_BLOCK = 1;

	private final LZ4Factory factory;

	private int compressionLevel;

	private FrameFormat_LZ4(LZ4Factory factory) {
		this.factory = factory;
	}

	public static FrameFormat_LZ4 create() {
		return new FrameFormat_LZ4(LZ4Factory.fastestInstance());
	}

	public static FrameFormat_LZ4 create(LZ4Factory factory) {
		return new FrameFormat_LZ4(factory);
	}

	public FrameFormat_LZ4 withHighCompression() {
		this.compressionLevel = -1;
		return this;
	}

	public FrameFormat_LZ4 withCompressionLevel(int compressionLevel) {
		checkArgument(compressionLevel >= -1);
		this.compressionLevel = compressionLevel;
		return this;
	}

	@Override
	public BlockEncoder createEncoder() {
		LZ4Compressor compressor = compressionLevel == 0 ?
				factory.fastCompressor() :
				compressionLevel == -1 ?
						factory.highCompressor() :
						factory.highCompressor(compressionLevel);
		return new BlockEncoder_LZ4(compressor);
	}

	@Override
	public BlockDecoder createDecoder() {
		return new BlockDecoder_LZ4(factory.fastDecompressor());
	}
}
