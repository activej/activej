package io.activej.redis;

import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.ApplicationSettings;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.net.Messaging;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static java.lang.Math.max;

final class RedisMessaging extends AbstractAsyncCloseable implements Messaging<RedisResponse, RedisCommand> {
	static final int INITIAL_BUFFER_SIZE = ApplicationSettings.getInt(RedisMessaging.class, "initialBufferSize", 16384);

	private final ByteBufQueue bufs = new ByteBufQueue();

	private final AsyncTcpSocket socket;
	private final RedisProtocol protocol;
	private final BinaryChannelSupplier bufsSupplier;

	private int bufferSize = INITIAL_BUFFER_SIZE;
	private ByteBuf buffer = ByteBufPool.allocate(bufferSize);

	private boolean readDone;
	private boolean writeDone;

	private boolean flushPosted;

	private RedisMessaging(AsyncTcpSocket socket, RedisProtocol protocol) {
		this.socket = socket;
		this.protocol = protocol;
		this.bufsSupplier = BinaryChannelSupplier.ofProvidedQueue(bufs,
				() -> this.socket.read()
						.then(buf -> {
							if (buf != null) {
								bufs.add(buf);
								return Promise.complete();
							} else {
								return Promise.ofException(UNEXPECTED_END_OF_STREAM_EXCEPTION);
							}
						})
						.whenException(this::closeEx),
				Promise::complete,
				this);
	}

	public static RedisMessaging create(AsyncTcpSocket socket, RedisProtocol protocol) {
		RedisMessaging redisMessaging = new RedisMessaging(socket, protocol);
		redisMessaging.prefetch();
		return redisMessaging;
	}

	private void prefetch() {
		if (bufs.isEmpty()) {
			socket.read()
					.whenResult(buf -> {
						if (buf != null) {
							bufs.add(buf);
						} else {
							readDone = true;
							closeIfDone();
						}
					})
					.whenException(this::closeEx);
		}
	}

	@Override
	public Promise<RedisResponse> receive() {
		return bufsSupplier.parse(protocol::tryDecode)
				.whenResult(this::prefetch)
				.whenException(this::closeEx);
	}

	@Override
	public Promise<Void> send(RedisCommand msg) {
		doEncode(msg);
		if (!flushPosted) {
			postFlush();
		}
		return Promise.complete();
	}

	private void doEncode(RedisCommand item) {
		int positionBegin;
		while (true) {
			positionBegin = buffer.tail();
			try {
				buffer.tail(protocol.encode(buffer.array(), buffer.tail(), item));
			} catch (ArrayIndexOutOfBoundsException e) {
				onUnderEstimate(positionBegin);
				continue;
			}
			break;
		}
		int positionEnd = buffer.tail();
		int dataSize = positionEnd - positionBegin;
		if (dataSize > bufferSize) {
			bufferSize = dataSize;
		}
	}

	private void onUnderEstimate(int positionBegin) {
		buffer.tail(positionBegin);
		int writeRemaining = buffer.writeRemaining();
		flush();
		buffer = ByteBufPool.allocate(max(bufferSize, writeRemaining + (writeRemaining >>> 1) + 1));
	}

	private void flush() {
		if (buffer.canRead()) {
			socket.write(buffer)
					.whenException(this::closeEx);
			if (bufferSize > INITIAL_BUFFER_SIZE){
				bufferSize = max(bufferSize - (bufferSize >>> 8), INITIAL_BUFFER_SIZE);
			}
		} else {
			buffer.recycle();
		}
		buffer = ByteBufPool.allocate(bufferSize);
	}

	private void postFlush() {
		flushPosted = true;
		eventloop.postLast(() -> {
			flushPosted = false;
			flush();
		});
	}

	@Override
	public Promise<Void> sendEndOfStream() {
		return socket.write(null)
				.whenResult(() -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(this::closeEx);
	}

	@Override
	public ChannelConsumer<ByteBuf> sendBinaryStream() {
		return ChannelConsumer.ofSocket(socket)
				.withAcknowledgement(ack -> ack
						.whenResult(() -> {
							writeDone = true;
							closeIfDone();
						}));
	}

	@Override
	public ChannelSupplier<ByteBuf> receiveBinaryStream() {
		return ChannelSuppliers.concat(ChannelSupplier.ofIterator(bufs.asIterator()), ChannelSupplier.ofSocket(socket))
				.withEndOfStream(eos -> eos
						.whenResult(() -> {
							readDone = true;
							closeIfDone();
						}));
	}

	@Override
	protected void onClosed(@NotNull Throwable e) {
		buffer.recycle();
		socket.closeEx(e);
		bufs.recycle();
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			close();
		}
	}
}
