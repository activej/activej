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

package io.activej.redis;

import io.activej.async.callback.Callback;
import io.activej.async.exception.AsyncCloseException;
import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.exception.MalformedDataException;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullify;
import static java.lang.Math.max;

public final class RedisConnection extends AbstractAsyncCloseable {
	private static final Logger logger = LoggerFactory.getLogger(RedisConnection.class);
	public static final boolean CHECKS = Checks.isEnabled(RedisConnection.class);

	static final int INITIAL_BUFFER_SIZE = ApplicationSettings.getInt(RedisConnection.class, "initialWriteBufferSize", 16384);

	private final RedisClient client;

	private ByteBuf readBuf = ByteBuf.empty();

	private final AsyncTcpSocket socket;

	private int estimatedSize;
	private int requiredRemainingSize;
	private @Nullable ByteBuf writeBuf = null;

	private boolean readDone;
	private boolean writeDone;

	@SuppressWarnings("rawtypes")
	private final ArrayDeque receiveQueue = new ArrayDeque<>();
	private List<Object> transactionQueue;

	private boolean flushPosted;
	private final int autoFlushIntervalMillis;

	RedisConnection(RedisClient client, AsyncTcpSocket socket, Duration autoFlushInterval) {
		this.client = client;
		this.socket = socket;
		this.autoFlushIntervalMillis = (int) autoFlushInterval.toMillis();
	}

	void start() {
		read();
	}

	// region Redis API

	/**
	 * Basic method that is used for sending any Redis command and parsing response.
	 * <p>
	 * May be used as a basis for implementing predefined Redis commands
	 *
	 * @param request  a request to be sent to a Redis server
	 * @param response an expected response parser
	 * @param <T>      type of parsed response
	 * @return promise of parsed response
	 */
	public <T> Promise<T> cmd(RedisRequest request, RedisResponse<T> response) {
		if (isClosed()) return Promise.ofException(new AsyncCloseException());

		int positionBegin, positionEnd;
		while (true) {
			if (writeBuf == null || writeBuf.writeRemaining() < requiredRemainingSize) {
				ensureBuffer();
			}
			positionBegin = writeBuf.tail();
			try {
				positionEnd = request.write(writeBuf.array(), positionBegin);
				writeBuf.tail(positionEnd);
			} catch (ArrayIndexOutOfBoundsException | NeedMoreDataException e) {
				enlargeBuffer();
				continue;
			}
			break;
		}
		int dataSize = positionEnd - positionBegin;
		if (dataSize > estimatedSize) {
			reestimate(dataSize);
		}
		return receive(response);
	}

	// region transactions

	/**
	 * Begins a new Redis transaction
	 *
	 * @return a promise of {@link Void} indicating that transaction has started.
	 * <b>Note, that it is not necessary to wait for this promise to resolve before issuing commands
	 * that should be part of a transaction (e.g. pipelined)</b>
	 * @throws IllegalStateException if another transaction is in progress
	 * @see <a href="https://redis.io/commands/multi">MULTI</a>
	 */
	public Promise<Void> multi() {
		if (CHECKS) checkState(!inTransaction(), "Nested MULTI call");
		logger.trace("Transaction has been started");
		Promise<Void> multiPromise = cmd(RedisRequest.of("MULTI"), RedisResponse.OK);
		this.transactionQueue = new ArrayList<>();
		return multiPromise;
	}

	/**
	 * Discards an active transaction. Any promise that waits for the result of the transaction
	 * will be completed exceptionally with {@link TransactionDiscardedException}
	 *
	 * @return a promise of {@link Void} indicating that transaction has been discarded.
	 * @throws IllegalStateException if there is no active transaction
	 * @see <a href="https://redis.io/commands/discard">DISCARD</a>
	 */
	public Promise<Void> discard() {
		if (CHECKS) checkState(inTransaction(), "DISCARD without MULTI");
		logger.trace("Transaction is being discarded");
		List<?> transactionQueue = this.transactionQueue;
		this.transactionQueue = null;
		int count = transactionQueue.size() / 2;
		if (count != 0) {
			TransactionDiscardedException e = new TransactionDiscardedException();
			for (int i = 0; i < count; i++) {
				SettablePromise<?> promise = (SettablePromise<?>) transactionQueue.get(i * 2 + 1);
				promise.trySetException(e);
			}
		}
		return cmd(RedisRequest.of("DISCARD"), RedisResponse.OK);
	}

	/**
	 * Executes all commands sent as part of an active transaction.
	 * <p>
	 * Once commands are executed and server sends a response, all the promises waiting for
	 * the result of the transaction will be completed with a corresponding result.
	 * If there are watched keys that have changed during this transaction, all the promises
	 * waiting for the result of the transaction will be completed exceptionally
	 * with {@link TransactionFailedException}.
	 *
	 * @return promise of an array containing all the results of commands
	 * issued during this transaction
	 * @throws IllegalStateException if there is no active transaction
	 * @see <a href="https://redis.io/commands/exec">EXEC</a>
	 */
	public Promise<Object[]> exec() {
		if (CHECKS) checkState(inTransaction(), "EXEC without MULTI");
		logger.trace("Executing transaction");
		List<?> transactionQueue = this.transactionQueue;
		this.transactionQueue = null;
		int count = transactionQueue.size() / 2;
		return cmd(RedisRequest.of("EXEC"),
				new RedisResponse<Object[]>() {
					@SuppressWarnings({"rawtypes", "unchecked"})
					@Override
					public Object[] parse(RESPv2 data) throws MalformedDataException {
						Object[] results = parseResponses(data);

						if (results == null) return null;

						for (int i = 0; i < count; i++) {
							SettablePromise promise = (SettablePromise) transactionQueue.get(2 * i + 1);
							Object result = results[i];
							if (result instanceof ServerError) {
								promise.trySetException((ServerError) result);
							} else {
								promise.set(result);
							}
						}
						return results;
					}

					private @Nullable Object @Nullable [] parseResponses(RESPv2 data) throws MalformedDataException {
						long len = data.readArraySize();
						if (len == -1) return null;

						if (len != count) throw new MalformedDataException(
								"Sent " + count + " requests in a transaction, got responses for " + len);

						Object[] results = new Object[count];

						byte[] array = data.array();
						for (int i = 0; i < count; i++) {
							if (!data.canRead()) throw NeedMoreDataException.NEED_MORE_DATA;
							RedisResponse<?> response = (RedisResponse<?>) transactionQueue.get(2 * i);

							if (array[data.head()] != RESPv2.ERROR_MARKER) {
								results[i] = response.parse(data);
							} else {
								results[i] = data.readObject();
							}
						}
						return results;
					}
				})
				.then(results -> results == null ?
						Promise.ofException(new TransactionFailedException()) :
						Promise.of(results))
				.whenException(e -> abortTransaction(transactionQueue, e));
	}

	/**
	 * Shows whether there is an active transaction in progress
	 */
	public boolean inTransaction() {
		return transactionQueue != null;
	}
	// endregion

	// region connection

	/**
	 * Gracefully ends current connection
	 *
	 * @return promise of {@link Void} indicating that connection has been closed
	 */
	public Promise<Void> quit() {
		if (transactionQueue != null) {
			QuitCalledException e = new QuitCalledException();
			List<?> transactionQueue = this.transactionQueue;
			this.transactionQueue = null;
			abortTransaction(transactionQueue, e);
		}
		return cmd(RedisRequest.of("QUIT"), RedisResponse.OK)
				.then(this::sendEndOfStream)
				.whenComplete(this::close);
	}
	// endregion
	// endregion

	@SuppressWarnings("unchecked")
	private <T> Promise<T> receive(RedisResponse<T> response) {
		SettablePromise<T> promise = new SettablePromise<>();
		if (transactionQueue == null) {
			receiveQueue.add(response);
			receiveQueue.add(promise);
		} else {
			receiveQueue.add(RedisResponse.QUEUED);
			receiveQueue.add((Callback<Void>) (result, e) -> {
				if (e != null) {
					promise.setException(e);
				}
			});

			this.transactionQueue.add(response);
			this.transactionQueue.add(promise);
		}

		return promise;
	}

	private void ensureBuffer() {
		flush();
		writeBuf = ByteBufPool.allocate(max(INITIAL_BUFFER_SIZE, requiredRemainingSize));
		if (!flushPosted) {
			postFlush();
		}
	}

	private void enlargeBuffer() {
		//noinspection ConstantConditions
		int writeRemaining = writeBuf.writeRemaining();
		flush();
		writeBuf = ByteBufPool.allocate(max(INITIAL_BUFFER_SIZE, writeRemaining + (writeRemaining >>> 1) + 1));
	}

	private void reestimate(int dataSize) {
		estimatedSize = dataSize;
		requiredRemainingSize = dataSize + (dataSize >>> 2);
	}

	private void postFlush() {
		flushPosted = true;
		if (autoFlushIntervalMillis <= 0) {
			reactor.postLast(() -> {
				flushPosted = false;
				flush();
			});
		} else {
			reactor.delayBackground(autoFlushIntervalMillis, () -> {
				flushPosted = false;
				flush();
			});
		}
	}

	private void flush() {
		if (writeBuf == null) return;
		if (writeBuf.canRead()) {
			socket.write(writeBuf)
					.whenException(e -> closeEx(new RedisException("Failed to write data", e)));
		} else {
			writeBuf.recycle();
		}
		writeBuf = null;
	}

	private Promise<Void> sendEndOfStream() {
		return socket.write(null)
				.whenResult(() -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(e -> closeEx(new RedisException("Failed to send end of stream", e)));
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	private void read() {
		socket.read()
				.whenResult(buf -> {
					if (buf != null) {
						readBuf = ByteBufPool.append(readBuf, buf);
						RESPv2 data = new RESPv2(readBuf.array(), readBuf.head(), readBuf.tail());

						int head = data.head();
						while (!receiveQueue.isEmpty() && data.canRead()) {
							RedisResponse<Object> response = (RedisResponse<Object>) receiveQueue.peek();
							try {
								if (data.peek() != RESPv2.ERROR_MARKER) {
									Object result = response.parse(data);
									head = data.head();
									receiveQueue.poll();
									((Callback<Object>) receiveQueue.poll()).accept(result, null);
								} else {
									ServerError error = (ServerError) data.readObject();
									head = data.head();
									receiveQueue.poll();
									((Callback<Object>) receiveQueue.poll()).accept(null, error);
								}
							} catch (NeedMoreDataException e) {
								break;
							} catch (MalformedDataException e) {
								closeEx(new RedisException(e));
								return;
							}
						}
						if (readBuf != null) {
							readBuf.head(head);
							if (!readBuf.canRead()) {
								readBuf.recycle();
								readBuf = ByteBuf.empty();
							}
						}
						read();
					} else {
						readDone = true;
						closeIfDone();
					}
				})
				.whenException(e -> closeEx(new RedisException("Failed to read data", e)));
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			close();
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "ConstantConditions"})
	protected void onClosed(Exception e) {
		socket.closeEx(e);
		writeBuf = nullify(writeBuf, ByteBuf::recycle);
		readBuf = nullify(readBuf, ByteBuf::recycle);
		while (!receiveQueue.isEmpty()) {
			receiveQueue.poll();
			((Callback<Object>) receiveQueue.poll()).accept(null, e);
		}
		transactionQueue = nullify(transactionQueue, queue -> abortTransaction(queue, e));
	}

	private void abortTransaction(List<?> transactionQueue, Exception e) {
		for (int i = 0; i < transactionQueue.size() / 2; i++) {
			SettablePromise<?> promise = (SettablePromise<?>) transactionQueue.get(2 * i + 1);
			promise.trySetException(e);
		}
	}

	@Override
	public String toString() {
		return "RedisConnection{" +
				"client=" + client +
				", receiveQueue=" + receiveQueue.size() / 2 +
				(transactionQueue != null ? (", transactionQueue=" + transactionQueue.size() / 2) : "") +
				", closed=" + isClosed() +
				'}';
	}
}
