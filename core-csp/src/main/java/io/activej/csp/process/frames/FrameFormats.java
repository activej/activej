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
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.binary.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.activej.common.Checks.checkArgument;
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
		return new IdentityFrameFormat();
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
		return new SizePrefixedFrameFormat();
	}

	/**
	 * A frame format that adds specified magic number to the start of the stream.
	 */
	public static FrameFormat withMagicNumber(FrameFormat frameFormat, byte[] magicNumber) {
		checkArgument(magicNumber.length != 0, "Empty magic number");
		return new MagicNumberAdapter(frameFormat, magicNumber);
	}

	// region implementations
	private static final class Compound implements FrameFormat {
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
			return new BlockDecoder() {
				BlockDecoder decoder;
				BlockDecoder possibleDecoder;
				Iterator<FrameFormat> possibleDecoders = formats.iterator();

				@Override
				public void reset() {
					if (decoder != null) {
						decoder.reset();
					}
				}

				@Override
				public boolean ignoreMissingEndOfStreamBlock() {
					if (decoder != null) return decoder.ignoreMissingEndOfStreamBlock();

					// rare case of empty stream
					return formats.stream().map(FrameFormat::createDecoder).anyMatch(BlockDecoder::ignoreMissingEndOfStreamBlock);
				}

				@Override
				public @Nullable ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
					if (decoder != null) return decoder.decode(bufs);
					return tryNextDecoder(bufs);
				}

				private ByteBuf tryNextDecoder(ByteBufs bufs) throws MalformedDataException {
					while (true) {
						if (possibleDecoder == null) {
							if (!possibleDecoders.hasNext()) throw new UnknownFormatException();
							possibleDecoder = possibleDecoders.next().createDecoder();
						}

						try {
							int bytesBeforeDecoding = bufs.remainingBytes();
							ByteBuf buf = possibleDecoder.decode(bufs);
							if (buf != null || bytesBeforeDecoding != bufs.remainingBytes()) {
								decoder = possibleDecoder;
								possibleDecoders = null;
							}
							return buf;
						} catch (MalformedDataException ignored) {
						}

						possibleDecoder = null;
					}
				}
			};
		}
	}

	private static final class IdentityFrameFormat implements FrameFormat {
		@Override
		public BlockEncoder createEncoder() {
			return new BlockEncoder() {
				@Override
				public ByteBuf encode(ByteBuf inputBuf) {
					return inputBuf.slice();
				}

				@Override
				public void reset() {
				}

				@Override
				public ByteBuf encodeEndOfStreamBlock() {
					return ByteBuf.empty();
				}
			};
		}

		@Override
		public BlockDecoder createDecoder() {
			return new BlockDecoder() {

				@Override
				public @Nullable ByteBuf decode(ByteBufs bufs) {
					return bufs.hasRemaining() ? bufs.takeRemaining() : null;
				}

				@Override
				public void reset() {
				}

				@Override
				public boolean ignoreMissingEndOfStreamBlock() {
					return true;
				}
			};
		}
	}

	private static final class SizePrefixedFrameFormat implements FrameFormat {
		private static final byte[] ZERO_BYTE_ARRAY = {0};

		@Override
		public BlockEncoder createEncoder() {
			return new BlockEncoder() {
				@Override
				public ByteBuf encode(ByteBuf inputBuf) {
					int len = inputBuf.readRemaining();
					ByteBuf outputBuf = ByteBufPool.allocate(len + 5);
					outputBuf.writeVarInt(len);
					outputBuf.put(inputBuf);
					return outputBuf;
				}

				@Override
				public void reset() {
				}

				@Override
				public ByteBuf encodeEndOfStreamBlock() {
					return ByteBuf.wrapForReading(ZERO_BYTE_ARRAY);
				}
			};
		}

		@Override
		public BlockDecoder createDecoder() {
			return new BlockDecoder() {
				private final LengthScanner lengthScanner = new LengthScanner();

				@Override
				public @Nullable ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
					int bytes = bufs.scanBytes(lengthScanner);
					if (bytes == 0) return null;
					int length = lengthScanner.value;
					if (length == 0) {
						bufs.skip(bytes);
						return END_OF_STREAM;
					}
					if (!bufs.hasRemainingBytes(bytes + length)) return null;
					bufs.skip(bytes);
					return bufs.takeExactSize(length);
				}

				@Override
				public void reset() {
				}

				@Override
				public boolean ignoreMissingEndOfStreamBlock() {
					return false;
				}
			};
		}

		private static final class LengthScanner implements ByteBufs.ByteScanner {
			int value;

			@Override
			public boolean consume(int index, byte b) throws MalformedDataException {
				value = index == 0 ? b & 0x7F : value | (b & 0x7F) << index * 7;
				if (b >= 0) {
					if (value < 0) throw new InvalidSizeException("Negative length");
					return true;
				}
				if (index == 4) throw new InvalidSizeException("Could not read var int");
				return false;
			}
		}
	}

	private static final class MagicNumberAdapter implements FrameFormat {
		private final FrameFormat peerFormat;
		private final byte[] magicNumber;
		private final ByteBufsDecoder<byte[]> magicNumberValidator;

		private MagicNumberAdapter(FrameFormat peerFormat, byte[] magicNumber) {
			this.peerFormat = peerFormat;
			this.magicNumber = magicNumber;
			this.magicNumberValidator = ByteBufsDecoder.assertBytes(magicNumber);
		}

		@Override
		public BlockEncoder createEncoder() {
			return new BlockEncoder() {
				final BlockEncoder peer = peerFormat.createEncoder();

				boolean writeMagicNumber = true;

				@Override
				public ByteBuf encode(ByteBuf inputBuf) {
					ByteBuf peerEncoded = peer.encode(inputBuf);
					if (writeMagicNumber) {
						writeMagicNumber = false;
						return ByteBufPool.append(ByteBuf.wrapForReading(magicNumber), peerEncoded);
					}
					return peerEncoded;
				}

				@Override
				public void reset() {
					writeMagicNumber = true;
				}

				@Override
				public ByteBuf encodeEndOfStreamBlock() {
					ByteBuf peerEncodedEndOfStream = peer.encodeEndOfStreamBlock();
					if (writeMagicNumber) {
						writeMagicNumber = false;
						return ByteBufPool.append(ByteBuf.wrapForReading(magicNumber), peerEncodedEndOfStream);
					}
					return peerEncodedEndOfStream;
				}
			};
		}

		@Override
		public BlockDecoder createDecoder() {
			return new BlockDecoder() {
				final BlockDecoder peer = peerFormat.createDecoder();

				boolean validateMagicNumber = true;

				@Override
				public ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
					if (validateMagicNumber) {
						if (magicNumberValidator.tryDecode(bufs) == null) return null;
						validateMagicNumber = false;
					}
					return peer.decode(bufs);
				}

				@Override
				public void reset() {
					validateMagicNumber = true;
				}

				@Override
				public boolean ignoreMissingEndOfStreamBlock() {
					return peer.ignoreMissingEndOfStreamBlock();
				}

			};
		}
	}
	// endregion
}
