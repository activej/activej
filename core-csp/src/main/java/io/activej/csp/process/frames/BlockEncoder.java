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

/**
 * Defines methods to encode incoming data into Data Blocks
 */
public interface BlockEncoder {
	/**
	 * Encodes input {@link ByteBuf} as a Data Block. Input bufs are not empty.
	 * If it is the first bytebuf, encoder may encode Stream Header together with Data Block.
	 *
	 * @param inputBuf buf to be encoded
	 * @return {@link ByteBuf} that contains encoded data
	 */
	ByteBuf encode(ByteBuf inputBuf);

	/**
	 * Attempts to reset some internal state of encoder.
	 * This method is called before each call to {@link #encode(ByteBuf)}
	 * (if {@link ChannelFrameEncoder} is configured to do so).
	 */
	void reset();

	/**
	 * Encodes an End-Of-Stream Block which indicates an end of stream.
	 * @return {@link ByteBuf} which contains End-Of-Stream block
	 */
	ByteBuf encodeEndOfStreamBlock();
}
