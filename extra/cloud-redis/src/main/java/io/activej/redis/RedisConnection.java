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
import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.exception.CloseException;
import io.activej.common.exception.MalformedDataException;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;

import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullify;
import static java.lang.Math.max;

public final class RedisConnection extends AbstractAsyncCloseable {
	private static final Logger logger = LoggerFactory.getLogger(RedisConnection.class);
	public static final boolean CHECK = Checks.isEnabled(RedisConnection.class);

	static final int INITIAL_BUFFER_SIZE = ApplicationSettings.getInt(RedisConnection.class, "initialWriteBufferSize", 16384);

	private final Eventloop eventloop;
	private final RedisClient client;

	private ByteBuf readBuf = ByteBuf.empty();

	private final AsyncTcpSocket socket;

	private int writeBufSize = INITIAL_BUFFER_SIZE;
	private ByteBuf writeBuf = ByteBufPool.allocate(writeBufSize);

	private boolean readDone;
	private boolean writeDone;

	@SuppressWarnings("rawtypes")
	private final ArrayDeque receiveQueue = new ArrayDeque<>();
	@SuppressWarnings("rawtypes")
	private ArrayList transactionQueue;

	RedisConnection(Eventloop eventloop, RedisClient client, AsyncTcpSocket socket) {
		this.eventloop = eventloop;
		this.client = client;
		this.socket = socket;
	}

	public <T> Promise<T> cmd(RedisRequest request, RedisResponse<T> response) {
		if (isClosed()) return Promise.ofException(new CloseException());

		int positionBegin, positionEnd;
		while (true) {
			positionBegin = writeBuf.tail();
			try {
				positionEnd = request.write(writeBuf.array(), positionBegin);
				writeBuf.tail(positionEnd);
			} catch (ArrayIndexOutOfBoundsException | NeedMoreDataException e) {
				onUnderEstimate(positionBegin);
				continue;
			}
			break;
		}
		int dataSize = positionEnd - positionBegin;
		if (dataSize > writeBufSize) {
			writeBufSize = dataSize;
		}
		return receive(response);
	}

	@SuppressWarnings({"unchecked"})
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

	private void onUnderEstimate(int positionBegin) {
		writeBuf.tail(positionBegin);
		int writeRemaining = writeBuf.writeRemaining();
		flush();
		writeBuf = ByteBufPool.allocate(max(writeBufSize, writeRemaining + (writeRemaining >>> 1) + 1));
	}

	private void flush() {
		if (writeBuf.canRead()) {
			socket.write(writeBuf)
					.whenException(this::closeEx);
			writeBufSize = max(writeBufSize - (writeBufSize >>> 8), INITIAL_BUFFER_SIZE);
		} else {
			writeBuf.recycle();
		}
		writeBuf = ByteBufPool.allocate(writeBufSize);
	}

	void start() {
		read();
		postFlush();
	}

	private void postFlush() {
		eventloop.postLast(() -> {
			if (!isClosed()) {
				flush();
				postFlush();
			}
		});
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	private void read() {
		socket.read()
				.whenResult(buf -> {
					if (buf != null) {
						readBuf = ByteBufPool.append(readBuf, buf);
						RESPv2 protocol = new RESPv2(readBuf.array(), readBuf.head(), readBuf.tail());

						while (!receiveQueue.isEmpty() && readBuf.canRead()) {
							RedisResponse<Object> response = (RedisResponse<Object>) receiveQueue.peek();
							try {
								if (readBuf.peek() != RESPv2.ERROR_MARKER) {
									Object result = response.parse(protocol);
									readBuf.head(protocol.head());
									receiveQueue.poll();
									((Callback<Object>) receiveQueue.poll()).accept(result, null);
								} else {
									ServerError error = (ServerError) protocol.readObject();
									readBuf.head(protocol.head());
									receiveQueue.poll();
									((Callback<Object>) receiveQueue.poll()).accept(null, error);
								}
							} catch (NeedMoreDataException e) {
								break;
							} catch (MalformedDataException e) {
								closeEx(e);
								return;
							}
						}
						read();
					} else {
						readDone = true;
						closeIfDone();
					}
				})
				.whenException(this::closeEx);
	}

	private Promise<Void> sendEndOfStream() {
		return socket.write(null)
				.whenResult(() -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(this::closeEx);
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	protected void onClosed(@NotNull Throwable e) {
		socket.closeEx(e);
		writeBuf = nullify(writeBuf, ByteBuf::recycle);
		readBuf = nullify(readBuf, ByteBuf::recycle);
		while (!receiveQueue.isEmpty()) {
			receiveQueue.poll();
			((Callback<Object>) receiveQueue.poll()).accept(null, e);
		}
		transactionQueue = nullify(transactionQueue, queue -> abortTransaction(queue, e));
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			close();
		}
	}

	// region Redis API

	// region transactions
	@SuppressWarnings("rawtypes")
	public Promise<Void> multi() {
		if (CHECK) checkState(!inTransaction(), "Nested MULTI call");
		logger.trace("Transaction has been started");
		Promise<Void> multiPromise = cmd(RedisRequest.of("MULTI"), RedisResponse.OK);
		this.transactionQueue = new ArrayList();
		return multiPromise;
	}

	@SuppressWarnings("rawtypes")
	public Promise<Void> discard() {
		if (CHECK) checkState(inTransaction(), "DISCARD without MULTI");
		logger.trace("Transaction is being discarded");
		ArrayList transactionQueue = this.transactionQueue;
		this.transactionQueue = null;
		int count = transactionQueue.size() / 2;
		TransactionDiscardedException e = new TransactionDiscardedException();
		for (int i = 0; i < count; i++) {
			SettablePromise promise = (SettablePromise) transactionQueue.get(i * 2 + 1);
			promise.trySetException(e);
		}
		return cmd(RedisRequest.of("DISCARD"), RedisResponse.OK);
	}

	public Promise<Object[]> exec() {
		if (CHECK) checkState(inTransaction(), "EXEC without MULTI");
		logger.trace("Executing transaction");
		//noinspection rawtypes
		ArrayList transactionQueue = this.transactionQueue;
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

					@Nullable
					private Object[] parseResponses(RESPv2 data) throws MalformedDataException {
						long len = data.readArraySize();
						if (len == -1) return null;

						if (len != count) throw new MalformedDataException();

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

	public boolean inTransaction() {
		return transactionQueue != null;
	}
	// endregion

	// region connection
	public Promise<Void> quit() {
		if (transactionQueue != null) {
			QuitCalledException e = new QuitCalledException();
			ArrayList<?> transactionQueue = this.transactionQueue;
			this.transactionQueue = null;
			abortTransaction(transactionQueue, e);
		}
		return cmd(RedisRequest.of("QUIT"), RedisResponse.OK)
				.then(this::sendEndOfStream)
				.whenComplete(this::close);
	}
	// endregion
	// endregion

	private void abortTransaction(ArrayList<?> transactionQueue, Throwable e) {
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
