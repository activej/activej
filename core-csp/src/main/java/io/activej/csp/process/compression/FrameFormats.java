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
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
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
		return IdentityFrameFormat.getInstance();
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
		return SizePrefixedFrameFormat.getInstance();
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
				public @Nullable ByteBuf decode(ByteBufQueue bufs) throws ParseException {
					if (decoder != null) return decoder.decode(bufs);
					return tryNextDecoder(bufs);
				}

				private ByteBuf tryNextDecoder(ByteBufQueue bufs) throws ParseException {
					while (true) {
						if (possibleDecoder == null) {
							if (!possibleDecoders.hasNext()) throw UNKNOWN_FORMAT_EXCEPTION;
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
						} catch (ParseException ignored) {
						}

						possibleDecoder = null;
					}
				}
			};
		}
	}

	private static final class IdentityFrameFormat implements FrameFormat {
		private static volatile IdentityFrameFormat instance = null;

		private IdentityFrameFormat() {
			if (instance != null) throw new AssertionError("Already created");
		}

		static IdentityFrameFormat getInstance() {
			if (instance == null) {
				synchronized (IdentityFrameFormat.class) {
					if (instance == null) {
						instance = new IdentityFrameFormat();
					}
				}
			}
			return instance;
		}

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
				public @Nullable ByteBuf decode(ByteBufQueue bufs) {
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
		private static final InvalidSizeException NEGATIVE_LENGTH = new InvalidSizeException(SizePrefixedFrameFormat.class, "Negative length");
		private static final byte[] ZERO_BYTE_ARRAY = {0};

		private static volatile SizePrefixedFrameFormat instance = null;

		private final byte[] varIntArray = new byte[5];

		private SizePrefixedFrameFormat() {
			if (instance != null) throw new AssertionError("Already created");
		}

		static SizePrefixedFrameFormat getInstance() {
			if (instance == null) {
				synchronized (SizePrefixedFrameFormat.class) {
					if (instance == null) {
						instance = new SizePrefixedFrameFormat();
					}
				}
			}
			return instance;
		}

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

				@Override
				public @Nullable ByteBuf decode(ByteBufQueue bufs) throws ParseException {
					int len = peekVarInt(bufs);
					if (len == -1) return null;

					int lenSize = varIntSize(len);
					if (!bufs.hasRemainingBytes(len + lenSize)) return null;
					if (len == 0) {
						bufs.skip(lenSize);
						return END_OF_STREAM;
					}
					ByteBuf buf = bufs.takeExactSize(len + lenSize);
					buf.moveHead(lenSize);
					return buf;
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

		private int peekVarInt(ByteBufQueue bufs) throws ParseException {
			ByteBuf buf = bufs.peekBuf();
			if (buf == null) return -1;

			byte[] array;
			int off, limit;
			if (buf.readRemaining() >= varIntArray.length) {
				array = buf.array();
				off = buf.head();
				limit = varIntArray.length;
			} else {
				limit = bufs.peekTo(varIntArray, 0, 5);
				array = varIntArray;
				off = 0;
			}

			int result;
			byte b = array[off++];
			if (b >= 0) {
				result = b;
			} else {
				if (limit == 1) return -1;
				result = b & 0x7f;
				if ((b = array[off++]) >= 0) {
					result |= b << 7;
				} else {
					if (limit == 2) return -1;
					result |= (b & 0x7f) << 7;
					if ((b = array[off++]) >= 0) {
						result |= b << 14;
					} else {
						if (limit == 3) return -1;
						result |= (b & 0x7f) << 14;
						if ((b = array[off++]) >= 0) {
							result |= b << 21;
						} else {
							if (limit == 4) return -1;
							result |= (b & 0x7f) << 21;
							if ((b = array[off]) >= 0) {
								result |= b << 28;
							} else {
								throw new InvalidSizeException(SizePrefixedFrameFormat.class, "Could not read var int");
							}
						}
					}
				}
			}
			if (result < 0) {
				throw NEGATIVE_LENGTH;
			}
			return result;
		}

		private static int varIntSize(int value) {
			return 1 + (31 - Integer.numberOfLeadingZeros(value)) / 7;
		}
	}

	private static final class MagicNumberAdapter implements FrameFormat {
		private final FrameFormat peerFormat;
		private final byte[] magicNumber;
		private final int magicNumberLength;
		private final byte[] magicNumberHolder;

		private MagicNumberAdapter(FrameFormat peerFormat, byte[] magicNumber) {
			this.peerFormat = peerFormat;
			this.magicNumber = magicNumber;
			this.magicNumberLength = magicNumber.length;
			this.magicNumberHolder = new byte[magicNumberLength];
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
				public ByteBuf decode(ByteBufQueue bufs) throws ParseException {
					if (validateMagicNumber) {
						if (!validateMagicNumber(bufs)) return null;
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

				private boolean validateMagicNumber(ByteBufQueue bufs) throws UnknownFormatException {
					final ByteBuf firstBuf = bufs.peekBuf();
					if (firstBuf == null) return false;

					byte[] array = firstBuf.array();
					int head = firstBuf.head();
					int tail = firstBuf.tail();

					int limit;
					if (tail - head < magicNumberLength) {
						limit = bufs.peekTo(magicNumberHolder, 0, magicNumberLength);
						array = magicNumberHolder;
						head = 0;
					} else {
						limit = magicNumberLength;
					}

					for (int i = 0; i < limit; i++) {
						if (array[head + i] != magicNumber[i]) {
							throw new UnknownFormatException(MagicNumberAdapter.class,
									"Expected stream to start with bytes: " + Arrays.toString(magicNumber));
						}
					}

					if (limit != magicNumberLength) return false;

					bufs.skip(magicNumberLength);
					return true;
				}
			};
		}
	}
	// endregion
}
