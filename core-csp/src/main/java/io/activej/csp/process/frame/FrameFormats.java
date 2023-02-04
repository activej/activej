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

import io.activej.common.annotation.StaticFactories;
import io.activej.csp.process.frame.impl.*;

import java.util.ArrayList;
import java.util.List;

import static io.activej.common.Checks.checkArgument;

@StaticFactories(FrameFormat.class)
public class FrameFormats {

	/**
	 * Creates a default LZ4 frame format
	 * <p>
	 * To create LZ4 frame format with desired configuration use {@link LZ4#builder()}
	 *
	 * @return LZ4 frame format
	 */
	public static FrameFormat lz4() {
		return LZ4.create();
	}

	/**
	 * Creates a default legacy LZ4 frame format
	 * <p>
	 * To create legacy LZ4 frame format with desired configuration use {@link LZ4Legacy#builder()}
	 *
	 * @return LZ4 frame format
	 */
	@Deprecated
	public static FrameFormat lz4Legacy() {
		return LZ4Legacy.create();
	}

	/**
	 * A combination of different frame formats.
	 * This {@link FrameFormat} encodes data using frame format passed as a first argument.
	 * Stream is decoded by the first decoder that can determine that data corresponds to decoder's format.
	 *
	 * @param mainFormat   a format that will be used for encoding data (will also be used as a candidate for decoding)
	 * @param otherFormats formats that are candidates for decoding data
	 * @return a compound frame format that consists of several other frame formats
	 */
	public static FrameFormat compound(FrameFormat mainFormat, FrameFormat... otherFormats) {
		List<FrameFormat> formats = new ArrayList<>();
		formats.add(mainFormat);
		formats.addAll(List.of(otherFormats));
		return new Compound(formats);
	}

	/**
	 * A frame format that does not change incoming data in any way.
	 * <p>
	 * <b>Should be used in {@link #compound(FrameFormat, FrameFormat...)} method only as the last frame format
	 * as it determines any data to have a correct format
	 * <p>
	 * You can wrap this frame format using {@link #withMagicNumber(FrameFormat, byte[])}
	 * by specifying custom magic number to be used
	 * </b>
	 */
	public static FrameFormat identity() {
		return new Identity();
	}

	/**
	 * A frame format that encodes data preceding it with its size.
	 * <p>
	 * <b>Should not be used in {@link #compound(FrameFormat, FrameFormat...)} method,
	 * as it does not determine its format (unless stream does not start with proper VarInt bytes)
	 * <p>
	 * You can wrap this frame format using {@link #withMagicNumber(FrameFormat, byte[])}
	 * by specifying custom magic number to be used
	 * </b>
	 */
	public static FrameFormat sizePrefixed() {
		return new SizePrefixed();
	}

	/**
	 * A frame format that adds specified magic number to the start of the stream.
	 */
	public static FrameFormat withMagicNumber(FrameFormat frameFormat, byte[] magicNumber) {
		checkArgument(magicNumber.length != 0, "Empty magic number");
		return new MagicNumberAdapter(frameFormat, magicNumber);
	}

}
