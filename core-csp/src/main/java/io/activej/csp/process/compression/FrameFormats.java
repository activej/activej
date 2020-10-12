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

package io.activej.csp.process.compression;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;

public class FrameFormats {

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
		return new Compound(mainFormat, asList(otherFormats));
	}

	private static final class Compound implements FrameFormat {
		private static final UnknownFormatException UNKNOWN_FORMAT_EXCEPTION = new UnknownFormatException(Compound.class);

		private final List<FrameFormat> formats = new ArrayList<>();

		Compound(FrameFormat mainFormat, List<FrameFormat> otherFormats) {
			formats.add(mainFormat);
			formats.addAll(otherFormats);
		}

		@Override
		public BlockEncoder createEncoder() {
			return formats.get(0).createEncoder();
		}

		@Override
		public BlockDecoder createDecoder() {
			final BlockDecoder[] decoders = formats.stream().map(FrameFormat::createDecoder).toArray(BlockDecoder[]::new);

			return new BlockDecoder() {
				BlockDecoder decoder;

				@Override
				public void reset() {
					if (decoder != null) {
						decoder.reset();
					}
				}

				@Override
				public @Nullable ByteBuf decode(ByteBufQueue bufs) throws ParseException {
					if (decoder != null) return decoder.decode(bufs);
					return findDecoder(bufs);
				}

				@Override
				public boolean ignoreMissingEndOfStreamBlock() {
					if (decoder != null) return decoder.ignoreMissingEndOfStreamBlock();

					// rare case of empty stream
					return Arrays.stream(decoders).anyMatch(BlockDecoder::ignoreMissingEndOfStreamBlock);
				}

				private ByteBuf findDecoder(ByteBufQueue bufs) throws ParseException {
					boolean needMoreData = false;
					for (int i = 0; i < decoders.length; i++) {
						BlockDecoder decoder = decoders[i];
						if (decoder == null) continue;
						try {
							int bytesBeforeDecoding = bufs.remainingBytes();
							ByteBuf buf = decoder.decode(bufs);
							if (buf != null || bytesBeforeDecoding != bufs.remainingBytes()) {
								this.decoder = decoder;
								return buf;
							} else {
								needMoreData = true;
							}
						} catch (ParseException ignored) {
							decoders[i] = null;
						}
					}
					if (needMoreData) {
						return null;
					}
					throw UNKNOWN_FORMAT_EXCEPTION;
				}
			};
		}
	}

}
