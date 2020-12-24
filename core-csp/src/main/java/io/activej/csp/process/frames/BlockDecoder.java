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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import org.jetbrains.annotations.Nullable;

/**
 * Defines methods to decode data encoded using as blocks
 */
public interface BlockDecoder {
	/**
	 * A 'sentinel' value that indicates end of stream.
	 * <b>It should never be used for anything other than indicating end of stream in {@link #decode(ByteBufQueue)} method</b>
	 */
	ByteBuf END_OF_STREAM = ByteBuf.wrap(new byte[0], 0, 0);

	/**
	 * Attempts to decode data blocks contained in a {@link ByteBufQueue}.
	 * <p>
	 * <b>Decoder is forbidden to consume any bytes from a queue if it has not yet determined that
	 * encoded data corresponds to decoder's format</b>
	 *
	 * @param bufs queue that contains encoded data
	 * @return a {@link ByteBuf} that contains successfully decoded data
	 * or {@code null} which indicates that there are not enough data in a queue for a block to be decoded.
	 * <p>
	 * If this method returns {@link #END_OF_STREAM} it indicates that end of stream has been reached
	 * and no more data is expected.
	 * @throws UnknownFormatException if data is encoded using unknown format
	 * @throws MalformedDataException         if data is malformed
	 */
	@Nullable
	ByteBuf decode(ByteBufQueue bufs) throws MalformedDataException;

	/**
	 * Attempts to reset some internal state of decoder.
	 * This method is called after each successfully decoded block
	 * (if {@link ChannelFrameDecoder} is configured to do so).
	 */
	void reset();

	/**
	 * Whether or not this decoder allows stream to end without End-Of-Stream Block.
	 * <p>
	 * <b>Stream still has to end with a valid complete Data Block (or be empty)
	 * and should not have any trailing data after the last block</b>
	 *
	 * @return whether decoder supports missing End-Of-Stream Blocks
	 */
	boolean ignoreMissingEndOfStreamBlock();

}
