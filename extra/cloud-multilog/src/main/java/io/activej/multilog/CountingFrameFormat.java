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

package io.activej.multilog;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.process.frames.BlockDecoder;
import io.activej.csp.process.frames.BlockEncoder;
import io.activej.csp.process.frames.FrameFormat;
import org.jetbrains.annotations.Nullable;

public final class CountingFrameFormat implements FrameFormat {
	private final FrameFormat frameFormat;

	private long blockOffset;
	private long innerOffset;

	public CountingFrameFormat(FrameFormat frameFormat) {
		this.frameFormat = frameFormat;
	}

	public long getCount() {
		return blockOffset;
	}

	public void resetCount() {
		blockOffset = 0;
		innerOffset = 0;
	}

	@Override
	public BlockEncoder createEncoder() {
		return frameFormat.createEncoder();
	}

	@Override
	public BlockDecoder createDecoder() {
		BlockDecoder decoder = frameFormat.createDecoder();
		return new BlockDecoder() {
			@Override
			public void reset() {
				decoder.reset();
			}

			@Override
			public @Nullable ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
				int before = bufs.remainingBytes();
				ByteBuf buf = decoder.decode(bufs);
				innerOffset += (before - bufs.remainingBytes());
				if (buf != null && buf != END_OF_STREAM) {
					blockOffset += innerOffset;
					innerOffset = 0;
				}
				return buf;
			}

			@Override
			public boolean ignoreMissingEndOfStreamBlock() {
				return decoder.ignoreMissingEndOfStreamBlock();
			}
		};
	}
}
