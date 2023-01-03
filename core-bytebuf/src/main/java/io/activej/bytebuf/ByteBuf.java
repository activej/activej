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

package io.activej.bytebuf;

import io.activej.common.Checks;
import io.activej.common.Utils;
import io.activej.common.recycle.Recyclable;
import org.jetbrains.annotations.Contract;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static java.lang.Math.min;

/**
 * Represents a wrapper over a byte array and has 2 positions: {@link #head} and {@link #tail}.
 * <p>
 * When you write data to {@code ByteBuf}, it's {@link #tail} increases by the amount of bytes written.
 * <p>
 * When you read data from {@code ByteBuf}, it's {@link #head} increases by the amount of bytes read.
 * <p>
 * You can read bytes from {@code ByteBuf} only when {@link #tail} is bigger than {@link #head}.
 * <p>
 * You can write bytes to {@code ByteBuf} until {@link #tail} doesn't exceed length of the underlying array.
 * <p>
 * ByteBuf is similar to a FIFO byte queue except it has no wrap-around or growth.
 */

@SuppressWarnings({"WeakerAccess", "DefaultAnnotationParam", "unused"})
public class ByteBuf implements Recyclable {
	private static final boolean CHECK = Checks.isEnabled(ByteBuf.class);

	static final boolean CHECK_RECYCLE = ByteBufPool.REGISTRY || CHECK;

	/**
	 * Allows creating slices of {@link ByteBuf}, helper class.
	 * <p>
	 * A slice is a wrapper over an original {@code ByteBuf}. A slice links to the same byte array as the original
	 * {@code ByteBuf}.
	 * <p>
	 * You still have to recycle original {@code ByteBuf} as well as all of its slices.
	 */
	static final class ByteBufSlice extends ByteBuf {
		private final ByteBuf root;

		private ByteBufSlice(ByteBuf buf, int head, int tail) {
			super(buf.array, head, tail);
			this.root = buf;
		}

		@Override
		public void recycle() {
			root.recycle();
		}

		@Override
		public void addRef() {
			root.addRef();
		}

		@Override
		public ByteBuf slice(int offset, int length) {
			return root.slice(offset, length);
		}

		@Override
		@Contract(pure = true)
		protected boolean isRecycled() {
			return root.isRecycled();
		}

		@Override
		@Contract(pure = true)
		protected boolean isRecycleNeeded() {
			return root.isRecycleNeeded();
		}
	}

	/**
	 * Stores bytes of this {@code ByteBuf}.
	 */
	protected final byte[] array;

	/**
	 * Stores <i>head</i> of this {@code ByteBuf}.
	 */
	int head;
	/**
	 * Stores <i>tail</i> of this {@code ByteBuf}.
	 */
	int tail;

	/**
	 * Shows whether this {@code ByteBuf} needs to be recycled.
	 * When you use {@link #slice()} this value increases by 1.
	 * When you use {@link #recycle()} this value decreases by 1.
	 * <p>
	 * This {@code ByteBuf} will be returned to the {@link ByteBufPool}
	 * only when this value equals 0.
	 */
	int refs;

	volatile int pos;

	/**
	 * Represents an empty ByteBuf with {@link #head} and
	 * {@link #tail} set at value 0.
	 */
	private static final ByteBuf EMPTY = wrap(new byte[0], 0, 0);

	// creators

	/**
	 * Creates a {@code ByteBuf} with custom byte array, {@link #tail} and {@link #head}.
	 *
	 * @param array byte array to be wrapped into {@code ByteBuf}
	 * @param head  value of {@link #head} of {@code ByteBuf}
	 * @param tail  value of {@link #tail} of {@code ByteBuf}
	 */
	private ByteBuf(byte[] array, int head, int tail) {
		if (CHECK) {
			checkArgument(head >= 0 && head <= tail && tail <= array.length,
					() -> "Wrong ByteBuf boundaries - readPos: " + head + ", writePos: " + tail + ", array.length: " + array.length);
		}
		this.array = array;
		this.head = head;
		this.tail = tail;
	}

	/**
	 * Creates an empty {@code ByteBuf} with array of size 0,
	 * {@link #tail} and {@link #head} both equal to 0.
	 *
	 * @return an empty {@code ByteBuf}
	 */
	@Contract(pure = true)
	public static ByteBuf empty() {
		if (CHECK) checkState(EMPTY.head == 0 && EMPTY.tail == 0);
		return EMPTY;
	}

	/**
	 * Wraps provided byte array into {@code ByteBuf} with {@link #tail} equal to 0.
	 *
	 * @param bytes byte array to be wrapped into {@code ByteBuf}
	 * @return {@code ByteBuf} over underlying byte array that is ready for writing
	 */
	@Contract("_ -> new")
	public static ByteBuf wrapForWriting(byte[] bytes) {
		return wrap(bytes, 0, 0);
	}

	/**
	 * Wraps provided byte array into {@code ByteBuf} with {@link #tail} equal to length of provided array.
	 *
	 * @param bytes byte array to be wrapped into {@code ByteBuf}
	 * @return {@code ByteBuf} over underlying byte array that is ready for reading
	 */
	@Contract("_ -> new")
	public static ByteBuf wrapForReading(byte[] bytes) {
		return wrap(bytes, 0, bytes.length);
	}

	/**
	 * Wraps provided byte array into {@code ByteBuf} with
	 * specified {@link #tail} and {@link #head}.
	 *
	 * @param bytes byte array to be wrapped into {@code ByteBuf}
	 * @param head  {@link #head} of {@code ByteBuf}
	 * @param tail  {@link #tail} of {@code ByteBuf}
	 * @return {@code ByteBuf} over underlying byte array with given {@link #tail} and {@link #head}
	 */
	@Contract("_, _, _ -> new")
	public static ByteBuf wrap(byte[] bytes, int head, int tail) {
		return new ByteBuf(bytes, head, tail);
	}

	// slicing

	/**
	 * Creates a slice of this {@code ByteBuf} if it is not recycled.
	 * Its {@link #head} and {@link #tail} won't change.
	 * <p>
	 * {@link #refs} increases by 1.
	 *
	 * @return a {@link ByteBufSlice} of this {@code ByteBuf}
	 */
	@Contract("-> new")
	public ByteBuf slice() {
		return slice(head, readRemaining());
	}

	/**
	 * Creates a slice of this {@code ByteBuf} with the given length if
	 * it is not recycled.
	 *
	 * @param length length of the new slice. Defines {@link #tail}
	 *               of the new {@link ByteBufSlice}.
	 *               It is added to the current {@link #head}.
	 * @return a {@code ByteBufSlice} of this {@code ByteBuf}.
	 */
	@Contract("_ -> new")
	public ByteBuf slice(int length) {
		return slice(head, length);
	}

	/**
	 * Creates a slice of this {@code ByteBuf} with the given offset and length.
	 *
	 * @param offset offset from which to slice this {@code ByteBuf}.
	 * @param length length of the slice.
	 * @return a {@code ByteBufSlice} of this {@code ByteBuf} with the given offset and length.
	 */
	@Contract("_, _ -> new")
	public ByteBuf slice(int offset, int length) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (!isRecycleNeeded()) {
			return ByteBuf.wrap(array, offset, offset + length);
		}
		refs++;
		return new ByteBufSlice(this, offset, offset + length);
	}

	// recycling

	/**
	 * Recycles this {@code ByteBuf} by returning it to {@link ByteBufPool}.
	 */
	@Override
	public void recycle() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (refs > 0 && --refs == 0) {
			if (CHECK_RECYCLE) refs = -1;
			ByteBufPool.recycle(this);
		}
	}

	/**
	 * Increases {@link #refs} value by 1.
	 */
	public void addRef() {
		refs++;
	}

	/**
	 * Checks if this {@code ByteBuf} is recycled.
	 *
	 * @return {@code true} or {@code false}
	 */
	@Contract(pure = true)
	protected boolean isRecycled() {
		return refs < 0;
	}

	/**
	 * Sets {@link #tail} and {@link #head} of this {@code ByteBuf} to 0.
	 */
	public void rewind() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		tail = 0;
		head = 0;
	}

	/**
	 * Checks if this {@code ByteBuf} needs recycling by checking the value of {@link #refs}.
	 * If the value is greater than 0, returns {@code true}.
	 *
	 * @return {@code true} if this {@code ByteBuf} needs recycle, otherwise {@code false}
	 */
	@Contract(pure = true)
	protected boolean isRecycleNeeded() {
		return refs > 0;
	}

	// byte buffers

	/**
	 * Wraps this {@code ByteBuf} into Java's {@link ByteBuffer} ready to read.
	 *
	 * @return {@link ByteBuffer} ready to read
	 */
	public ByteBuffer toReadByteBuffer() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return ByteBuffer.wrap(array, head, readRemaining());
	}

	/**
	 * Wraps this {@code ByteBuf} into Java's {@link ByteBuffer} ready to write.
	 *
	 * @return {@link ByteBuffer} ready to write
	 */
	public ByteBuffer toWriteByteBuffer() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return ByteBuffer.wrap(array, tail, writeRemaining());
	}

	/**
	 * Unwraps given Java's {@link ByteBuffer} into {@code ByteBuf}.
	 *
	 * @param byteBuffer {@link ByteBuffer} to be unwrapped
	 */
	public void ofReadByteBuffer(ByteBuffer byteBuffer) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) checkArgument(array == byteBuffer.array() && byteBuffer.limit() == tail);
		head = byteBuffer.position();
	}

	/**
	 * Unwraps given Java's {@link ByteBuffer} into {@code ByteBuf}.
	 *
	 * @param byteBuffer {@link ByteBuffer} to be unwrapped
	 */
	public void ofWriteByteBuffer(ByteBuffer byteBuffer) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) checkArgument(array == byteBuffer.array() && byteBuffer.limit() == array.length);
		tail = byteBuffer.position();
	}

	// getters & setters

	/**
	 * Returns byte array {@link #array}.
	 *
	 * @return {@link #array}
	 */
	@Contract(value = "->!null", pure = true)
	public byte[] array() {
		return array;
	}

	/**
	 * Returns length of the {@link #array} of this {@code ByteBuf}.
	 *
	 * @return length of this {@code ByteBuf}
	 */
	@Contract(pure = true)
	public int limit() {
		return array.length;
	}

	/**
	 * Returns {@link #head} if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@link #head}
	 */
	@Contract(pure = true)
	public int head() {
		return head;
	}

	/**
	 * Sets {@link #head} if this {@code ByteBuf} is not recycled.
	 *
	 * @param pos the value which will be assigned to the {@link #head}.
	 *            Must be smaller or equal to {@link #tail}
	 */
	public void head(int pos) {
		if (CHECK) checkArgument(pos <= tail);
		head = pos;
	}

	/**
	 * Returns {@link #tail} if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@link #tail}
	 */
	@Contract(pure = true)
	public int tail() {
		return tail;
	}

	/**
	 * Sets {@link #tail} if this {@code ByteBuf} is not recycled.
	 *
	 * @param pos the value which will be assigned to the {@link #tail}.
	 *            Must be bigger or equal to {@link #head}
	 *            and smaller than length of the {@link #array}
	 */
	public void tail(int pos) {
		if (CHECK) checkArgument(pos >= head && pos <= array.length);
		tail = pos;
	}

	/**
	 * Sets new value of {@link #head} by moving it by the given delta
	 * if this {@link ByteBuf} is not recycled.
	 *
	 * @param delta the value by which current {@link #head} will be moved.
	 *              New {@link #head} must be bigger or equal to 0
	 *              and smaller or equal to {@link #tail}
	 */
	public void moveHead(int delta) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) {
			checkArgument(head + delta >= 0, "New head cannot be negative");
			checkArgument(head + delta <= tail, "New head cannot be greater than tail");
		}
		head += delta;
	}

	/**
	 * Sets new value of {@link #tail} by moving it by the given delta
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @param delta the value by which current {@link #tail} will be moved.
	 *              New {@link #tail} must be bigger or equal to {@link #head}
	 *              and smaller or equal to the length of the {@link #array}
	 */
	public void moveTail(int delta) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) {
			checkArgument(tail + delta >= head, "New tail cannot be lesser than head");
			checkArgument(tail + delta <= array.length, "New tail cannot be greater than size of underlying array");
		}
		tail += delta;
	}

	/**
	 * Returns the amount of bytes which are available for reading
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return amount of bytes available for reading
	 */
	@Contract(pure = true)
	public int readRemaining() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return tail - head;
	}

	/**
	 * Returns the amount of bytes which are available for writing
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return amount of bytes available for writing
	 */
	@Contract(pure = true)
	public int writeRemaining() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return array.length - tail;
	}

	/**
	 * Checks if there are bytes available for reading
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@code true} if {@link #head} doesn't equal
	 * {@link #tail}, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean canRead() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return head != tail;
	}

	/**
	 * Checks if there are bytes available for writing
	 * if this {@code ByteBuf} is not recycled.
	 *
	 * @return {@code true} if {@link #tail} doesn't equal the
	 * length of the {@link #array}, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean canWrite() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return tail != array.length;
	}

	/**
	 * Returns the byte from this {@code ByteBuf} which index is equal
	 * to {@link #head}. Then increases {@link #head} by 1.
	 *
	 * @return {@code byte} value at the {@link #head}
	 */
	public byte get() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) checkState(head < tail, "No bytes are remaining for read");
		return array[head++];
	}

	/**
	 * Returns byte from this {@code ByteBuf} which index is equal
	 * to the passed value. Then increases {@link #head} by 1.
	 *
	 * @param index index of the byte to be returned.
	 * @return the {@code byte} at the specified position.
	 */
	@Contract(pure = true)
	public byte at(int index) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return array[index];
	}

	/**
	 * Returns a {@code byte} from this {@link #array} which is
	 * located at {@link #head} if this {@code ByteBuf} is not recycled.
	 *
	 * @return a {@code byte} from this {@link #array} which is
	 * located at {@link #head}.
	 */
	@Contract(pure = true)
	public byte peek() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return array[head];
	}

	/**
	 * Returns a {@code byte} from this {@link #array} which is
	 * located at {@link #head} position increased by the offset
	 * if this {@code ByteBuf} is not recycled. {@link #head} doesn't
	 * change.
	 *
	 * @param offset added to the {@link #head} value. Received value
	 *               must be smaller than current {@link #tail}.
	 * @return a {@code byte} from this {@link #array} which is
	 * located at {@link #head} with provided offset
	 */
	@Contract(pure = true)
	public byte peek(int offset) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) checkArgument(head + offset < tail, "Trying to peek outside of buf's limit");
		return array[head + offset];
	}

	/**
	 * Drains bytes from this {@code ByteBuf} starting from current {@link #head}
	 * to a given byte array with specified offset and length.
	 * <p>
	 * This {@code ByteBuf} must be not recycled.
	 *
	 * @param array  array to which bytes will be drained to
	 * @param offset starting position in the destination data.
	 *               The sum of the value and the {@code length} parameter
	 *               must be smaller or equal to {@link #array} length
	 * @param length number of bytes to be drained to given array.
	 *               Must be greater or equal to 0.
	 *               The sum of the value and {@link #head}
	 *               must be smaller or equal to {@link #tail}
	 * @return number of bytes that were drained.
	 */
	public int drainTo(byte[] array, int offset, int length) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) {
			checkArgument(length >= 0, () -> "Length should be a positive value");
			checkArgument(offset + length <= array.length && head + length <= tail,
					"Trying to drain from outside this buf's limit");
		}
		System.arraycopy(this.array, head, array, offset, length);
		head += length;
		return length;
	}

	/**
	 * Drains bytes to a given {@code ByteBuf}.
	 *
	 * @see #drainTo(byte[], int, int)
	 */
	public int drainTo(ByteBuf buf, int length) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) {
			checkArgument(buf.tail + length <= buf.array.length,
					"Trying to drain from outside this buf's limit");
		}
		drainTo(buf.array, buf.tail, length);
		buf.tail += length;
		return length;
	}

	/**
	 * Sets given {@code byte} at particular position of the
	 * {@link #array} if this {@code ByteBuf} is not recycled.
	 *
	 * @param index the index of the {@link #array} where
	 *              the given {@code byte} will be set
	 * @param b     the byte to be set at the given index of the {@link #array}
	 */
	public void set(int index, byte b) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		array[index] = b;
	}

	/**
	 * Puts given {@code byte} to the {@link #array} at the {@link #tail}
	 * and increases the {@link #tail} by 1.
	 *
	 * @param b the byte which will be put to the {@link #array}.
	 */
	public void put(byte b) {
		set(tail, b);
		tail++;
	}

	/**
	 * Puts given ByteBuf to this {@code ByteBuf} from the {@link #tail}
	 * and increases the {@link #tail} by the length of the given ByteBuf.
	 * Then equates ByteBuf's {@link #head} to {@link #tail}.
	 * <p>
	 * Only those bytes which are located between {@link #head}
	 * and {@link #tail} are put to the {@link #array}.
	 *
	 * @param buf the ByteBuf which will be put to the {@code ByteBuf}
	 */
	public void put(ByteBuf buf) {
		put(buf.array, buf.head, buf.tail - buf.head);
		buf.head = buf.tail;
	}

	/**
	 * Puts given byte array to the {@link #array} at the {@link #tail}
	 * and increases the {@link #tail} by the length of the given array.
	 *
	 * @param bytes the byte array which will be put to the {@link #array}
	 */
	public void put(byte[] bytes) {
		put(bytes, 0, bytes.length);
	}

	/**
	 * Puts given byte array to the {@link ByteBuf} from the {@link #tail}
	 * with given offset.
	 * Increases the {@link #tail} by the length of the given array.
	 * <p>
	 * This {@link ByteBuf} must be not recycled.
	 * Its length must be greater or equal to the sum of its {@link #tail}
	 * and the length of the byte array which will be put in it.
	 * Also, the sum of the provided offset and length of the byte array which will
	 * be put to the {@link #array} must smaller or equal to the whole length of the byte array.
	 *
	 * @param bytes  the byte array which will be put to the {@link #array}
	 * @param offset value of the offset in the byte array
	 * @param length length of the byte array which
	 *               will be put to the {@link #array}
	 */
	public void put(byte[] bytes, int offset, int length) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if (CHECK) {
			checkArgument(tail + length <= array.length, () -> "This buf cannot hold " + length + " more bytes");
			checkArgument(offset + length <= bytes.length, "Not enough bytes in source array");
		}
		System.arraycopy(bytes, offset, array, tail, length);
		tail += length;
	}

	/**
	 * Finds the given value in the {@link #array} and returns its position.
	 * <p>
	 * This {@code ByteBuf} must be not recycled.
	 *
	 * @param b the {@code byte} which is to be found in the {@link #array}
	 * @return position of byte in the {@link #array}. If the byte wasn't found,
	 * returns -1
	 */
	public int find(byte b) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		for (int i = head; i < tail; i++) {
			if (array[i] == b) return i;
		}
		return -1;
	}

	/**
	 * Finds the given byte array in the {@link #array} and returns its position.
	 * This {@code ByteBuf} must be not recycled.
	 *
	 * @param bytes the byte array which is to be found in the {@link #array}
	 * @return the position of byte array in the {@link #array}. If the byte wasn't found,
	 * returns -1
	 */
	public int find(byte[] bytes) {
		return find(bytes, 0, bytes.length);
	}

	/**
	 * Finds the given byte array in the {@link #array} and returns its position.
	 * This {@link ByteBuf} must be not recycled.
	 *
	 * @param bytes the byte array which is to be found in the {@link #array}
	 * @param off   offset in the byte array
	 * @param len   amount of the bytes to be found
	 * @return the position of byte array in the {@link #array}. If the byte wasn't found,
	 * returns -1
	 */
	public int find(byte[] bytes, int off, int len) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		L:
		for (int pos = head; pos <= tail - len; pos++) {
			for (int i = 0; i < len; i++) {
				if (array[pos + i] != bytes[off + i]) {
					continue L;
				}
			}
			return pos;
		}
		return -1;
	}

	/**
	 * Checks if provided array is equal to the readable bytes of the {@link #array}.
	 *
	 * @param array  byte array to be compared with the {@link #array}
	 * @param offset offset value for the provided byte array
	 * @param length amount of the bytes to be compared
	 * @return {@code true} if the byte array is equal to the array, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean isContentEqual(byte[] array, int offset, int length) {
		return Utils.arraysEquals(this.array, head, readRemaining(), array, offset, length);
	}

	/**
	 * Checks if provided {@code ByteBuf} readable bytes are equal to the
	 * readable bytes of the {@link #array}.
	 *
	 * @param other {@code ByteBuf} to be compared with the {@link #array}
	 * @return {@code true} if the {@code ByteBuf} is equal to the array,
	 * otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean isContentEqual(ByteBuf other) {
		return isContentEqual(other.array, other.head, other.readRemaining());
	}

	/**
	 * Checks if provided array is equal to the readable bytes of the {@link #array}.
	 *
	 * @param array byte array to be compared with the {@link #array}
	 * @return {@code true} if the byte array is equal to the array, otherwise {@code false}
	 */
	@Contract(pure = true)
	public boolean isContentEqual(byte[] array) {
		return isContentEqual(array, 0, array.length);
	}

	/**
	 * Returns a byte array from {@link #head} to {@link #tail}.
	 * Doesn't recycle this {@link ByteBuf}.
	 *
	 * @return byte array from {@link #head} to {@link #tail}
	 */
	@Contract(value = "->!null", pure = true)
	public byte[] getArray() {
		byte[] bytes = new byte[readRemaining()];
		System.arraycopy(array, head, bytes, 0, bytes.length);
		return bytes;
	}

	/**
	 * Returns a byte array from {@link #head} to {@link #tail}.
	 * DOES recycle this {@code ByteBuf}.
	 *
	 * @return byte array created from this {@code ByteBuf}
	 */
	@Contract(value = "->!null", pure = false)
	public byte[] asArray() {
		byte[] bytes = getArray();
		recycle();
		return bytes;
	}

	/**
	 * Returns a {@code String} created from this {@code ByteBuf} using given charset.
	 * Does not recycle this {@code ByteBuf}.
	 *
	 * @param charset charset which is used to create {@code String} from this {@code ByteBuf}.
	 * @return {@code String} from this {@code ByteBuf} in a given charset.
	 */
	@Contract(pure = true)
	public String getString(Charset charset) {
		return new String(array, head, readRemaining(), charset);
	}

	/**
	 * Returns a {@code String} created from this {@code ByteBuf} using given charset.
	 * DOES recycle this {@code ByteBuf}.
	 *
	 * @param charset charset which is used to create string from {@code ByteBuf}
	 * @return {@code String} from this {@code ByteBuf} in a given charset.
	 */
	@Contract(pure = false)
	public String asString(Charset charset) {
		String string = getString(charset);
		recycle();
		return string;
	}

	// region serialization input

	public int read(byte[] b) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return drainTo(b, off, len);
	}

	public byte readByte() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return array[head++];
	}

	public boolean readBoolean() {
		return readByte() != 0;
	}

	public char readChar() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		char c = (char) (((array[head] & 0xFF) << 8) | (array[head + 1] & 0xFF));
		head += 2;
		return c;
	}

	public double readDouble() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return Double.longBitsToDouble(readLong());
	}

	public float readFloat() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		return Float.intBitsToFloat(readInt());
	}

	public int readInt() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		int result = ((array[head] & 0xFF) << 24)
				| ((array[head + 1] & 0xFF) << 16)
				| ((array[head + 2] & 0xFF) << 8)
				| (array[head + 3] & 0xFF);
		head += 4;
		return result;
	}

	public int readVarInt() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		int result;
		byte b = array[head];
		if (b >= 0) {
			result = b;
			head += 1;
		} else {
			result = b & 0x7f;
			if ((b = array[head + 1]) >= 0) {
				result |= b << 7;
				head += 2;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = array[head + 2]) >= 0) {
					result |= b << 14;
					head += 3;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = array[head + 3]) >= 0) {
						result |= b << 21;
						head += 4;
					} else {
						result |= (b & 0x7f) << 21;
						if ((b = array[head + 4]) >= 0) {
							result |= b << 28;
							head += 5;
						} else {
							throw new IllegalStateException("Read varint was too long");
						}
					}
				}
			}
		}
		return result;
	}

	public long readLong() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		long result = ((long) array[head] << 56)
				| ((long) (array[head + 1] & 0xFF) << 48)
				| ((long) (array[head + 2] & 0xFF) << 40)
				| ((long) (array[head + 3] & 0xFF) << 32)
				| ((long) (array[head + 4] & 0xFF) << 24)
				| ((array[head + 5] & 0xFF) << 16)
				| ((array[head + 6] & 0xFF) << 8)
				| (array[head + 7] & 0xFF);
		head += 8;
		return result;
	}

	public short readShort() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		short result = (short) (((array[head] & 0xFF) << 8)
				| (array[head + 1] & 0xFF));
		head += 2;
		return result;
	}

	public long readVarLong() {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readByte();
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0)
				return result;
		}
		throw new IllegalStateException("Read varint was too long");
	}
	// endregion

	// region serialization output
	public void write(byte[] b) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		write(b, 0, b.length);
	}

	public void write(byte[] b, int off, int len) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		System.arraycopy(b, off, array, tail, len);
		tail = tail + len;
	}

	public void writeBoolean(boolean v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		writeByte(v ? (byte) 1 : 0);
	}

	public void writeByte(byte v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		array[tail] = v;
		tail = tail + 1;
	}

	public void writeChar(char v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		array[tail] = (byte) (v >>> 8);
		array[tail + 1] = (byte) v;
		tail = tail + 2;
	}

	public void writeDouble(double v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		writeLong(Double.doubleToLongBits(v));
	}

	public void writeFloat(float v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		writeInt(Float.floatToIntBits(v));
	}

	public void writeInt(int v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		array[tail] = (byte) (v >>> 24);
		array[tail + 1] = (byte) (v >>> 16);
		array[tail + 2] = (byte) (v >>> 8);
		array[tail + 3] = (byte) v;
		tail = tail + 4;
	}

	public void writeLong(long v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		int high = (int) (v >>> 32);
		int low = (int) v;
		array[tail] = (byte) (high >>> 24);
		array[tail + 1] = (byte) (high >>> 16);
		array[tail + 2] = (byte) (high >>> 8);
		array[tail + 3] = (byte) high;
		array[tail + 4] = (byte) (low >>> 24);
		array[tail + 5] = (byte) (low >>> 16);
		array[tail + 6] = (byte) (low >>> 8);
		array[tail + 7] = (byte) low;
		tail = tail + 8;
	}

	public void writeShort(short v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		array[tail] = (byte) (v >>> 8);
		array[tail + 1] = (byte) v;
		tail = tail + 2;
	}

	public void writeVarInt(int v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if ((v & ~0x7F) == 0) {
			array[tail] = (byte) v;
			tail += 1;
			return;
		}
		array[tail] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[tail + 1] = (byte) v;
			tail += 2;
			return;
		}
		array[tail + 1] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[tail + 2] = (byte) v;
			tail += 3;
			return;
		}
		array[tail + 2] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[tail + 3] = (byte) v;
			tail += 4;
			return;
		}
		array[tail + 3] = (byte) (v | 0x80);
		v >>>= 7;
		array[tail + 4] = (byte) v;
		tail += 5;
	}

	public void writeVarLong(long v) {
		if (CHECK_RECYCLE && isRecycled()) throw ByteBufPool.onByteBufRecycled(this);
		if ((v & ~0x7F) == 0) {
			array[tail] = (byte) v;
			tail += 1;
			return;
		}
		array[tail] = (byte) (v | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			array[tail + 1] = (byte) v;
			tail += 2;
			return;
		}
		array[tail + 1] = (byte) (v | 0x80);
		v >>>= 7;
		tail += 2;
		for (; ; ) {
			if ((v & ~0x7FL) == 0) {
				writeByte((byte) v);
				return;
			} else {
				writeByte((byte) (v | 0x80));
				v >>>= 7;
			}
		}
	}

	// endregion

	@Override
	@Contract(pure = true)
	public String toString() {
		char[] chars = new char[min(tail - head, 256)];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[head + i];
			chars[i] = (b == '\n') ? (char) 9166 : (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}
}
