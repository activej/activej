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

package io.activej.csp.process.frame;

/**
 * This interface specifies data encoding format which utilizes block structure.
 * <p>
 * A format may or may not compress data that is being encoded.
 * <p>
 * A typical format starts with a Stream Header that contains 'magic' bytes
 * required to identify a stream format and some initial settings for the stream.
 * <p>
 * Stream Header is followed by 0 to N Data Blocks which may have their own headers and footers.
 * <p>
 * A stream ends with some special End-Of-Stream Block
 */
public interface FrameFormat {
	/**
	 * Creates an encoder which transforms input bufs into bufs of encoded data
	 *
	 * @return block encoder
	 */
	BlockEncoder createEncoder();

	/**
	 * Creates a decoder which decodes data encoded by {@link BlockEncoder}
	 *
	 * @return block decoder
	 */
	BlockDecoder createDecoder();
}
