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

package io.activej.eventloop;

import io.activej.async.callback.AsyncComputation;
import io.activej.async.callback.Callback;
import io.activej.common.Checks;
import io.activej.common.api.WithInitializer;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.UncheckedException;
import io.activej.common.inspector.BaseInspector;
import io.activej.common.reflection.ReflectionUtils;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.common.time.CurrentTimeProviderSystem;
import io.activej.common.time.Stopwatch;
import io.activej.eventloop.error.FatalErrorHandler;
import io.activej.eventloop.error.FatalErrorHandlers;
import io.activej.eventloop.executor.EventloopExecutor;
import io.activej.eventloop.inspector.EventloopInspector;
import io.activej.eventloop.inspector.EventloopStats;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.eventloop.net.DatagramSocketSettings;
import io.activej.eventloop.net.ServerSocketSettings;
import io.activej.eventloop.schedule.ScheduledRunnable;
import io.activej.eventloop.schedule.Scheduler;
import io.activej.eventloop.util.OptimizedSelectedKeysSet;
import io.activej.eventloop.util.RunnableWithContext;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullToSupplier;
import static io.activej.eventloop.util.Utils.tryToOptimizeSelector;
import static java.util.Collections.emptyIterator;

/**
 * It is an internal class for asynchronous programming. In asynchronous
 * programming model, blocking operations (like I/O or long-running computations)
 * in {@code Eventloop} thread must be avoided. Async versions
 * of such operations should be used.
 * <p>
 * Eventloop represents infinite loop with only one blocking operation
 * {@code selector.select()} which selects a set of keys whose corresponding
 * channels are ready for I/O operations.
 * <p>
 * With these keys and queues with
 * tasks, which were added to {@code Eventloop} from the outside, it begins
 * asynchronous executing from one thread in method {@code run()} which is
 * overridden because eventloop is an implementation of {@link Runnable}.
 * Working of this eventloop will be ended when it has no selected keys
 * and its queues with tasks are empty.
 */
public final class Eventloop implements Runnable, EventloopExecutor, Scheduler, WithInitializer<Eventloop>, EventloopJmxBeanEx {
	public static final Logger logger = LoggerFactory.getLogger(Eventloop.class);
	private static final boolean CHECK = Checks.isEnabled(Eventloop.class);

	public static final boolean JIGSAW_DETECTED;
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(1);
	public static final Duration DEFAULT_IDLE_INTERVAL = Duration.ofSeconds(1);

	static {
		JIGSAW_DETECTED = ReflectionUtils.isClassPresent("java.lang.Module");
	}

	private static final AsyncTimeoutException CONNECT_TIMEOUT = new AsyncTimeoutException(Eventloop.class, "Connection timed out");

	@NotNull
	private static volatile FatalErrorHandler globalFatalErrorHandler = FatalErrorHandlers.ignoreAllErrors();

	/**
	 * Collection of local tasks which were added from this thread.
	 */
	private final ArrayDeque<Runnable> localTasks = new ArrayDeque<>();

	private final ArrayList<Runnable> nextTasks = new ArrayList<>();

	/**
	 * Collection of concurrent tasks which were added from other threads.
	 */
	private final ConcurrentLinkedQueue<Runnable> concurrentTasks = new ConcurrentLinkedQueue<>();

	/**
	 * Collection of scheduled tasks that are scheduled
	 * to be executed at particular timestamp.
	 */
	private final PriorityQueue<ScheduledRunnable> scheduledTasks = new PriorityQueue<>();

	/**
	 * Collection of background tasks,
	 * if eventloop contains only background tasks, it will be closed.
	 */
	private final PriorityQueue<ScheduledRunnable> backgroundTasks = new PriorityQueue<>();

	/**
	 * Amount of concurrent operations in other threads,
	 * non-zero value prevents eventloop from termination.
	 */
	private final AtomicInteger externalTasksCount = new AtomicInteger(0);

	private int loop;
	private int tick;

	/**
	 * Current time, cached to avoid System.currentTimeMillis()
	 * system calls, and to facilitate unit testing.
	 * It is being refreshed with each eventloop execution.
	 */
	private long timestamp;

	@NotNull
	private final CurrentTimeProvider timeProvider;

	/**
	 * The NIO selector which selects a set of keys whose
	 * corresponding channels are ready for I/O operations.
	 */
	@Nullable
	private Selector selector;

	@Nullable
	private SelectorProvider selectorProvider;

	/**
	 * The thread in which eventloop is running.
	 */
	@Nullable
	private Thread eventloopThread;

	private static final ThreadLocal<Eventloop> CURRENT_EVENTLOOP = new ThreadLocal<>();
	/**
	 * The desired name of the thread.
	 */
	@Nullable
	private String threadName;
	private int threadPriority;

	@Nullable
	private FatalErrorHandler fatalErrorHandler;

	private volatile boolean keepAlive;
	private volatile boolean breakEventloop;

	private Duration idleInterval = DEFAULT_IDLE_INTERVAL;

	/**
	 * Amount of selected keys for last Selector.select()
	 */
	private int lastSelectedKeys;
	private int cancelledKeys;
	private int lastExternalTasksCount;

	// JMX

	@Nullable
	private EventloopInspector inspector;

	private boolean monitoring = false;

	// region builders
	private Eventloop(@NotNull CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		refreshTimestamp();
	}

	public static Eventloop create() {
		return create(CurrentTimeProviderSystem.instance());
	}

	public static Eventloop create(@NotNull CurrentTimeProvider currentTimeProvider) {
		return new Eventloop(currentTimeProvider);
	}

	@NotNull
	public Eventloop withThreadName(@Nullable String threadName) {
		this.threadName = threadName;
		return this;
	}

	@SuppressWarnings("UnusedReturnValue")
	@NotNull
	public Eventloop withThreadPriority(int threadPriority) {
		this.threadPriority = threadPriority;
		return this;
	}

	@NotNull
	public Eventloop withInspector(@Nullable EventloopInspector inspector) {
		this.inspector = inspector;
		return this;
	}

	@NotNull
	public Eventloop withFatalErrorHandler(@Nullable FatalErrorHandler fatalErrorHandler) {
		this.fatalErrorHandler = fatalErrorHandler;
		return this;
	}

	@NotNull
	public Eventloop withSelectorProvider(@Nullable SelectorProvider selectorProvider) {
		this.selectorProvider = selectorProvider;
		return this;
	}

	@NotNull
	public Eventloop withIdleInterval(@NotNull Duration idleInterval) {
		this.idleInterval = idleInterval;
		return this;
	}

	@NotNull
	public Eventloop withCurrentThread() {
		CURRENT_EVENTLOOP.set(this);
		return this;
	}

	// endregion

	@Nullable
	public Selector getSelector() {
		return selector;
	}

	private static final String NO_CURRENT_EVENTLOOP_ERROR = "Trying to start async operations prior eventloop.run(), or from outside of eventloop.run() \n" +
			"Possible solutions: " +
			"1) Eventloop.create().withCurrentThread() ... {your code block} ... eventloop.run() \n" +
			"2) try_with_resources Eventloop.useCurrentThread() ... {your code block} \n" +
			"3) refactor application so it starts async operations within eventloop.run(), \n" +
			"   i.e. by implementing EventloopService::start() {your code block} and using ServiceGraphModule";

	@NotNull
	public static Eventloop getCurrentEventloop() {
		Eventloop eventloop = CURRENT_EVENTLOOP.get();
		if (eventloop != null) return eventloop;
		throw new IllegalStateException(NO_CURRENT_EVENTLOOP_ERROR);
	}

	public static void initWithEventloop(@NotNull Eventloop anotherEventloop, @NotNull Runnable runnable) {
		Eventloop eventloop = CURRENT_EVENTLOOP.get();
		try {
			CURRENT_EVENTLOOP.set(anotherEventloop);
			runnable.run();
		} finally {
			CURRENT_EVENTLOOP.set(eventloop);
		}
	}

	public static <T> T initWithEventloop(@NotNull Eventloop anotherEventloop, @NotNull Supplier<T> callable) {
		Eventloop eventloop = CURRENT_EVENTLOOP.get();
		try {
			CURRENT_EVENTLOOP.set(anotherEventloop);
			return callable.get();
		} finally {
			CURRENT_EVENTLOOP.set(eventloop);
		}
	}

	private void openSelector() {
		if (selector == null) {
			try {
				selector = nullToSupplier(selectorProvider, SelectorProvider::provider).openSelector();
			} catch (Exception e) {
				logger.error("Could not open selector", e);
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Closes the selector if it is opened.
	 */
	private void closeSelector() {
		if (selector != null) {
			try {
				selector.close();
				selector = null;
				cancelledKeys = 0;
			} catch (IOException e) {
				logger.error("Could not close selector", e);
			}
		}
	}

	@Nullable
	public Selector ensureSelector() {
		if (selector == null) {
			openSelector();
		}
		return selector;
	}

	public void closeChannel(@Nullable SelectableChannel channel, @Nullable SelectionKey key) {
		checkArgument(channel != null || key == null, "Either channel or key should be not null");
		if (channel == null || !channel.isOpen()) return;
		if (key != null && key.isValid()) {
			cancelledKeys++;
		}
		try {
			channel.close();
		} catch (IOException e) {
			logger.warn("Failed to close channel {}", channel, e);
		}
	}

	public boolean inEventloopThread() {
		return eventloopThread == null || eventloopThread == Thread.currentThread();
	}

	/**
	 * Sets the flag which (if set {@code true})
	 * means that working of this Eventloop will
	 * be continued even if all of the tasks
	 * have been executed and it doesn't have
	 * any selected keys.
	 *
	 * @param keepAlive flag for setting
	 */
	public void keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		if (!keepAlive && selector != null) {
			selector.wakeup();
		}
	}

	public void breakEventloop() {
		breakEventloop = true;
		if (breakEventloop && selector != null) {
			selector.wakeup();
		}
	}

	private boolean isAlive() {
		if (breakEventloop)
			return false;
		lastExternalTasksCount = externalTasksCount.get();
		return !localTasks.isEmpty() || !scheduledTasks.isEmpty() || !concurrentTasks.isEmpty()
				|| lastExternalTasksCount > 0
				|| keepAlive || (selector != null && selector.isOpen() && selector.keys().size() - cancelledKeys > 0);
	}

	@Nullable
	public Thread getEventloopThread() {
		return eventloopThread;
	}

	/**
	 * Overridden method from Runnable that executes tasks while this eventloop is alive.
	 */
	@Override
	public void run() {
		eventloopThread = Thread.currentThread();
		if (threadName != null)
			eventloopThread.setName(threadName);
		if (threadPriority != 0)
			eventloopThread.setPriority(threadPriority);
		CURRENT_EVENTLOOP.set(this);
		ensureSelector();
		assert selector != null;
		breakEventloop = false;
		boolean setWasOptimized = !JIGSAW_DETECTED && tryToOptimizeSelector(selector);

		long timeAfterSelectorSelect;
		long timeAfterBusinessLogic = 0;
		while (isAlive()) {
			try {
				long selectTimeout = getSelectTimeout();
				if (inspector != null) inspector.onUpdateSelectorSelectTimeout(selectTimeout);
				if (selectTimeout <= 0) {
					lastSelectedKeys = selector.selectNow();
				} else {
					lastSelectedKeys = selector.select(selectTimeout);
				}
				cancelledKeys = 0;
			} catch (ClosedChannelException e) {
				logger.error("Selector is closed, exiting...", e);
				break;
			} catch (IOException e) {
				recordIoError(e, selector);
			}

			timeAfterSelectorSelect = refreshTimestampAndGet();
			int keys = setWasOptimized ?
					optimizedProcessSelectedKeys((OptimizedSelectedKeysSet) selector.selectedKeys()) :
					processSelectedKeys(selector.selectedKeys());
			int concurrentTasks = executeConcurrentTasks();
			int scheduledTasks = executeScheduledTasks();
			int backgroundTasks = executeBackgroundTasks();
			int localTasks = executeLocalTasks();

			if (inspector != null) {
				if (timeAfterBusinessLogic != 0) {
					long selectorSelectTime = timeAfterSelectorSelect - timeAfterBusinessLogic;
					inspector.onUpdateSelectorSelectTime(selectorSelectTime);
				}

				timeAfterBusinessLogic = timestamp;
				boolean taskOrKeyPresent = (keys + concurrentTasks + scheduledTasks + backgroundTasks + localTasks) != 0;
				boolean externalTaskPresent = lastExternalTasksCount != 0;
				long businessLogicTime = timeAfterBusinessLogic - timeAfterSelectorSelect;
				inspector.onUpdateBusinessLogicTime(taskOrKeyPresent, externalTaskPresent, businessLogicTime);
			}

			loop++;
			tick = 0;
		}
		logger.info("{} finished", this);
		eventloopThread = null;
		if (selector != null && selector.isOpen() && selector.keys().stream().anyMatch(SelectionKey::isValid)) {
			logger.warn("Selector is still open, because event loop {} has {} keys", this, selector.keys());
			return;
		}
		closeSelector();
	}

	private long getSelectTimeout() {
		if (!concurrentTasks.isEmpty() || !localTasks.isEmpty())
			return 0L;
		if (scheduledTasks.isEmpty() && backgroundTasks.isEmpty())
			return idleInterval.toMillis();
		return Math.min(getTimeBeforeExecution(scheduledTasks), getTimeBeforeExecution(backgroundTasks));
	}

	private long getTimeBeforeExecution(PriorityQueue<ScheduledRunnable> taskQueue) {
		while (!taskQueue.isEmpty()) {
			ScheduledRunnable first = taskQueue.peek();
			if (first.isCancelled()) {
				taskQueue.poll();
				continue;
			}
			return first.getTimestamp() - currentTimeMillis();
		}
		return idleInterval.toMillis();
	}

	/**
	 * Processes selected keys related to various I/O events: accept, connect, read, write.
	 *
	 * @param selectedKeys set that contains all selected keys, returned from NIO Selector.select()
	 */
	private int processSelectedKeys(@NotNull Set<SelectionKey> selectedKeys) {
		long startTimestamp = timestamp;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		int invalidKeys = 0, acceptKeys = 0, connectKeys = 0, readKeys = 0, writeKeys = 0;

		Iterator<SelectionKey> iterator = lastSelectedKeys != 0 ? selectedKeys.iterator() : emptyIterator();
		while (iterator.hasNext()) {
			SelectionKey key = iterator.next();
			iterator.remove();

			if (!key.isValid()) {
				invalidKeys++;
				continue;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			if (key.isAcceptable()) {
				onAccept(key);
				acceptKeys++;
			} else if (key.isConnectable()) {
				onConnect(key);
				connectKeys++;
			} else {
				if (key.isReadable()) {
					onRead(key);
					readKeys++;
				}
				if (key.isValid()) {
					if (key.isWritable()) {
						onWrite(key);
						writeKeys++;
					}
				} else {
					invalidKeys++;
				}
			}
			if (sw != null && inspector != null) inspector.onUpdateSelectedKeyDuration(sw);
		}

		int keys = acceptKeys + connectKeys + readKeys + writeKeys + invalidKeys;

		if (keys != 0) {
			long loopTime = refreshTimestampAndGet() - startTimestamp;
			if (inspector != null)
				inspector.onUpdateSelectedKeysStats(lastSelectedKeys, invalidKeys, acceptKeys, connectKeys, readKeys, writeKeys, loopTime);
		}

		return keys;
	}

	private int optimizedProcessSelectedKeys(@NotNull OptimizedSelectedKeysSet selectedKeys) {
		long startTimestamp = timestamp;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		int invalidKeys = 0, acceptKeys = 0, connectKeys = 0, readKeys = 0, writeKeys = 0;

		for (int i = 0; i < selectedKeys.size(); i++) {
			SelectionKey key = selectedKeys.get(i);
			if (!key.isValid()) {
				invalidKeys++;
				continue;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			if (key.isAcceptable()) {
				onAccept(key);
				acceptKeys++;
			} else if (key.isConnectable()) {
				onConnect(key);
				connectKeys++;
			} else {
				if (key.isReadable()) {
					onRead(key);
					readKeys++;
				}
				if (key.isValid()) {
					if (key.isWritable()) {
						onWrite(key);
						writeKeys++;
					}
				} else {
					invalidKeys++;
				}
			}
			if (sw != null && inspector != null) inspector.onUpdateSelectedKeyDuration(sw);
		}
		selectedKeys.clear();
		int keys = acceptKeys + connectKeys + readKeys + writeKeys + invalidKeys;

		if (keys != 0) {
			long loopTime = refreshTimestampAndGet() - startTimestamp;
			if (inspector != null)
				inspector.onUpdateSelectedKeysStats(lastSelectedKeys, invalidKeys, acceptKeys, connectKeys, readKeys, writeKeys, loopTime);
		}

		return keys;
	}

	private static void executeTask(@Async.Execute Runnable task) {
		task.run();
	}

	/**
	 * Executes local tasks which were added from current thread
	 */
	private int executeLocalTasks() {
		long startTimestamp = timestamp;

		int localTasks = 0;

		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		while (true) {
			Runnable runnable = this.localTasks.poll();
			if (runnable == null) {
				break;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				executeTask(runnable);
				tick++;
				if (sw != null && inspector != null) inspector.onUpdateLocalTaskDuration(runnable, sw);
			} catch (Throwable e) {
				onFatalError(e, runnable);
			}
			localTasks++;
		}

		this.localTasks.addAll(nextTasks);
		this.nextTasks.clear();

		if (localTasks != 0) {
			long loopTime = refreshTimestampAndGet() - startTimestamp;
			if (inspector != null) inspector.onUpdateLocalTasksStats(localTasks, loopTime);
		}

		return localTasks;
	}

	/**
	 * Executes concurrent tasks which were added from other threads.
	 */
	private int executeConcurrentTasks() {
		long startTimestamp = timestamp;

		int concurrentTasks = 0;

		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		while (true) {
			Runnable runnable = this.concurrentTasks.poll();
			if (runnable == null) {
				break;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				executeTask(runnable);
				if (sw != null && inspector != null) inspector.onUpdateConcurrentTaskDuration(runnable, sw);
			} catch (Throwable e) {
				onFatalError(e, runnable);
			}
			concurrentTasks++;
		}

		if (concurrentTasks != 0) {
			long loopTime = refreshTimestampAndGet() - startTimestamp;
			if (inspector != null) inspector.onUpdateConcurrentTasksStats(concurrentTasks, loopTime);
		}

		return concurrentTasks;
	}

	/**
	 * Executes tasks scheduled for execution at particular timestamps
	 */
	private int executeScheduledTasks() {
		return executeScheduledTasks(scheduledTasks);
	}

	private int executeBackgroundTasks() {
		return executeScheduledTasks(backgroundTasks);
	}

	private int executeScheduledTasks(PriorityQueue<ScheduledRunnable> taskQueue) {
		long startTimestamp = timestamp;
		boolean background = taskQueue == backgroundTasks;

		int scheduledTasks = 0;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		for (; ; ) {
			ScheduledRunnable peeked = taskQueue.peek();
			if (peeked == null)
				break;
			if (peeked.isCancelled()) {
				taskQueue.poll();
				continue;
			}
			if (peeked.getTimestamp() > currentTimeMillis()) {
				break;
			}
			taskQueue.poll();

			Runnable runnable = peeked.getRunnable();
			if (sw != null) {
				sw.reset();
				sw.start();
			}

			if (monitoring && inspector != null) {
				int overdue = (int) (System.currentTimeMillis() - peeked.getTimestamp());
				inspector.onScheduledTaskOverdue(overdue, background);
			}

			try {
				executeTask(runnable);
				tick++;
				peeked.complete();
				if (sw != null && inspector != null) inspector.onUpdateScheduledTaskDuration(runnable, sw, background);
			} catch (Throwable e) {
				onFatalError(e, runnable);
			}

			scheduledTasks++;
		}

		if (scheduledTasks != 0) {
			long loopTime = refreshTimestampAndGet() - startTimestamp;
			if (inspector != null) inspector.onUpdateScheduledTasksStats(scheduledTasks, loopTime, background);
		}

		return scheduledTasks;
	}

	/**
	 * Accepts an incoming socketChannel connections without blocking eventloop thread.
	 *
	 * @param key key of this action.
	 */
	private void onAccept(SelectionKey key) {
		assert inEventloopThread();

		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
		if (!serverSocketChannel.isOpen()) { // TODO - remove?
			key.cancel();
			return;
		}

		//noinspection unchecked
		Consumer<SocketChannel> acceptCallback = (Consumer<SocketChannel>) key.attachment();
		for (; ; ) {
			SocketChannel channel;
			try {
				channel = serverSocketChannel.accept();
				if (channel == null)
					break;
				channel.configureBlocking(false);
			} catch (ClosedChannelException e) {
				break;
			} catch (IOException e) {
				recordIoError(e, serverSocketChannel);
				break;
			}

			try {
				acceptCallback.accept(channel);
			} catch (Throwable e) {
				recordFatalError(e, acceptCallback);
				closeChannel(channel, null);
			}
		}
	}

	/**
	 * Processes newly established TCP connections
	 * without blocking eventloop thread.
	 *
	 * @param key key of this action.
	 */
	private void onConnect(SelectionKey key) {
		assert inEventloopThread();
		@SuppressWarnings("unchecked") Callback<SocketChannel> cb = (Callback<SocketChannel>) key.attachment();
		SocketChannel channel = (SocketChannel) key.channel();
		boolean connected;
		try {
			connected = channel.finishConnect();
		} catch (IOException e) {
			closeChannel(channel, key);
			cb.accept(null, e);
			return;
		}

		try {
			if (connected) {
				cb.accept(channel, null);
			} else {
				cb.accept(null, new IOException("Connection key was received but the channel was not connected - " +
						"this is not possible without some bug in Java NIO"));
			}
		} catch (Throwable e) {
			recordFatalError(e, channel);
			closeChannel(channel, null);
		}
	}

	/**
	 * Processes socketChannels available for read
	 * without blocking event loop thread.
	 *
	 * @param key key of this action.
	 */
	private void onRead(SelectionKey key) {
		assert inEventloopThread();
		NioChannelEventHandler handler = (NioChannelEventHandler) key.attachment();
		try {
			handler.onReadReady();
		} catch (Throwable e) {
			recordFatalError(e, handler);
			closeChannel(key.channel(), null);
		}
	}

	/**
	 * Processes socketChannels available for write
	 * without blocking thread.
	 *
	 * @param key key of this action.
	 */
	private void onWrite(SelectionKey key) {
		assert inEventloopThread();
		NioChannelEventHandler handler = (NioChannelEventHandler) key.attachment();
		try {
			handler.onWriteReady();
		} catch (Throwable e) {
			recordFatalError(e, handler);
			closeChannel(key.channel(), null);
		}
	}

	/**
	 * Creates {@link ServerSocketChannel} that listens on InetSocketAddress.
	 *
	 * @param address              InetSocketAddress that server will listen to
	 * @param serverSocketSettings settings from this server channel
	 * @param acceptCallback       callback that is called when new incoming connection is being accepted. It can be called multiple times.
	 * @return server channel
	 * @throws IOException If some I/O error occurs
	 */
	@NotNull
	public ServerSocketChannel listen(@Nullable InetSocketAddress address, @NotNull ServerSocketSettings serverSocketSettings, @NotNull Consumer<SocketChannel> acceptCallback) throws IOException {
		if (CHECK) checkState(inEventloopThread(), "Not in eventloop thread");
		ServerSocketChannel serverSocketChannel = null;
		try {
			serverSocketChannel = ServerSocketChannel.open();
			serverSocketSettings.applySettings(serverSocketChannel);
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.bind(address, serverSocketSettings.getBacklog());
			serverSocketChannel.register(ensureSelector(), SelectionKey.OP_ACCEPT, acceptCallback);
			if (selector != null) {
				selector.wakeup();
			}
			return serverSocketChannel;
		} catch (IOException e) {
			if (serverSocketChannel != null) {
				closeChannel(serverSocketChannel, null);
			}
			throw e;
		}
	}

	/**
	 * Registers new UDP connection in this eventloop.
	 *
	 * @param bindAddress address for binding DatagramSocket for this connection.
	 * @return DatagramSocket of this connection
	 * @throws IOException if an I/O error occurs on opening DatagramChannel
	 */
	@NotNull
	public static DatagramChannel createDatagramChannel(DatagramSocketSettings datagramSocketSettings,
			@Nullable InetSocketAddress bindAddress,
			@Nullable InetSocketAddress connectAddress) throws IOException {
		DatagramChannel datagramChannel = null;
		try {
			datagramChannel = DatagramChannel.open();
			datagramSocketSettings.applySettings(datagramChannel);
			datagramChannel.configureBlocking(false);
			datagramChannel.bind(bindAddress);
			if (connectAddress != null) {
				datagramChannel.connect(connectAddress);
			}
			return datagramChannel;
		} catch (IOException e) {
			if (datagramChannel != null) {
				try {
					datagramChannel.close();
				} catch (Exception nested) {
					logger.error("Failed closing datagram channel after I/O error", nested);
					e.addSuppressed(nested);
				}
			}
			throw e;
		}
	}

	/**
	 * Connects to given socket address asynchronously.
	 *
	 * @param address socketChannel's address
	 */
	public void connect(SocketAddress address, @NotNull Callback<SocketChannel> cb) {
		connect(address, 0, cb);
	}

	public void connect(SocketAddress address, @Nullable Duration timeout, @NotNull Callback<SocketChannel> cb) {
		connect(address, timeout == null ? 0L : timeout.toMillis(), cb);
	}

	/**
	 * Connects to given socket address asynchronously with a specified timeout value.
	 * A timeout of zero is interpreted as an default system timeout
	 *
	 * @param address socketChannel's address
	 * @param timeout the timeout value to be used in milliseconds, 0 as default system connection timeout
	 */
	public void connect(@NotNull SocketAddress address, long timeout, @NotNull Callback<SocketChannel> cb) {
		if (CHECK) checkState(inEventloopThread(), "Not in eventloop thread");
		SocketChannel channel;
		try {
			channel = SocketChannel.open();
		} catch (IOException e) {
			try {
				cb.accept(null, e);
			} catch (Throwable e1) {
				recordFatalError(e1, cb);
			}
			return;
		}
		try {
			channel.configureBlocking(false);
			channel.connect(address);

			if (timeout == 0) {
				channel.register(ensureSelector(), SelectionKey.OP_CONNECT, cb);
			} else {
				ScheduledRunnable scheduledTimeout = delay(timeout, () -> {
					closeChannel(channel, null);
					cb.accept(null, CONNECT_TIMEOUT);
				});

				channel.register(ensureSelector(), SelectionKey.OP_CONNECT,
						(Callback<SocketChannel>) (result, e) -> {
							scheduledTimeout.cancel();
							cb.accept(result, e);
						});
			}

			if (selector != null) {
				selector.wakeup();
			}
		} catch (IOException e) {
			closeChannel(channel, null);
			try {
				cb.accept(null, e);
			} catch (Throwable e1) {
				recordFatalError(e1, cb);
			}
		}
	}

	public long tick() {
		if (CHECK) checkState(inEventloopThread(), "Not in eventloop thread");
		return (long) loop << 32 | tick;
	}

	/**
	 * Posts a new task to the beginning of localTasks.
	 * This method is recommended, since task will be executed
	 * as soon as possible without invalidating CPU cache.
	 *
	 * @param runnable runnable of this task
	 */
	public void post(@NotNull @Async.Schedule Runnable runnable) {
		if (CHECK) checkState(inEventloopThread(), "Not in eventloop thread");
		localTasks.addFirst(runnable);
	}

	/**
	 * Posts a new task to the end of localTasks.
	 *
	 * @param runnable runnable of this task
	 */
	public void postLast(@NotNull @Async.Schedule Runnable runnable) {
		if (CHECK) checkState(inEventloopThread(), "Not in eventloop thread");
		localTasks.addLast(runnable);
	}

	public void postNext(@NotNull @Async.Schedule Runnable runnable) {
		if (CHECK) checkState(inEventloopThread(), "Not in eventloop thread");
		nextTasks.add(runnable);
	}

	/**
	 * Posts a new task from other threads.
	 * This is the preferred method of communicating
	 * with eventloop from other threads.
	 *
	 * @param runnable runnable of this task
	 */
	@Override
	public void execute(@NotNull @Async.Schedule Runnable runnable) {
		concurrentTasks.offer(runnable);
		if (selector != null) {
			selector.wakeup();
		}
	}

	/**
	 * Schedules new task. Returns {@link ScheduledRunnable} with this runnable.
	 *
	 * @param timestamp timestamp after which task will be ran
	 * @param runnable  runnable of this task
	 * @return scheduledRunnable, which could used for cancelling the task
	 */
	@NotNull
	@Override
	public ScheduledRunnable schedule(long timestamp, @NotNull @Async.Schedule Runnable runnable) {
		if (CHECK) checkState(inEventloopThread(), "Not in eventloop thread");
		return addScheduledTask(timestamp, runnable, false);
	}

	/**
	 * Schedules new background task. Returns {@link ScheduledRunnable} with this runnable.
	 * <p>
	 * If eventloop contains only background tasks, it will be closed
	 *
	 * @param timestamp timestamp after which task will be ran
	 * @param runnable  runnable of this task
	 * @return scheduledRunnable, which could used for cancelling the task
	 */
	@NotNull
	@Override
	public ScheduledRunnable scheduleBackground(long timestamp, @NotNull @Async.Schedule Runnable runnable) {
		if (CHECK)
			checkState(inEventloopThread(), "Not in eventloop thread");
		return addScheduledTask(timestamp, runnable, true);
	}

	@NotNull
	private ScheduledRunnable addScheduledTask(long timestamp, Runnable runnable, boolean background) {
		ScheduledRunnable scheduledTask = ScheduledRunnable.create(timestamp, runnable);
		PriorityQueue<ScheduledRunnable> taskQueue = background ? backgroundTasks : scheduledTasks;
		taskQueue.offer(scheduledTask);
		return scheduledTask;
	}

	/**
	 * Notifies the eventloop about concurrent operation in other threads.
	 * Eventloop will not exit until all external tasks are complete.
	 */
	public void startExternalTask() {
		externalTasksCount.incrementAndGet();
	}

	/**
	 * Notifies the eventloop about completion of corresponding operation in other threads.
	 * Failure to call this method will prevent the eventloop from exiting.
	 */
	public void completeExternalTask() {
		externalTasksCount.decrementAndGet();
	}

	public long refreshTimestampAndGet() {
		refreshTimestamp();
		return timestamp;
	}

	private void refreshTimestamp() {
		timestamp = timeProvider.currentTimeMillis();
	}

	/**
	 * Returns current time of this eventloop
	 */
	@Override
	public long currentTimeMillis() {
		return timestamp;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return this;
	}

	/**
	 * Submits {@code Runnable} to eventloop for execution
	 * <p>{@code Runnable} is executed in the eventloop thread</p>
	 *
	 * @param computation to be executed
	 * @return {@code CompletableFuture} that completes when runnable completes
	 */
	@NotNull
	@Override
	public CompletableFuture<Void> submit(@NotNull Runnable computation) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		execute(() -> {
			try {
				computation.run();
			} catch (UncheckedException u) {
				future.completeExceptionally(u.getCause());
				return;
			}
			future.complete(null);
		});
		return future;
	}

	@NotNull
	@Override
	public <T> CompletableFuture<T> submit(Supplier<? extends AsyncComputation<T>> computation) {
		CompletableFuture<T> future = new CompletableFuture<>();
		execute(() -> {
			try {
				computation.get().run((result, e) -> {
					if (e == null) {
						future.complete(result);
					} else {
						future.completeExceptionally(e);
					}
				});
			} catch (UncheckedException u) {
				future.completeExceptionally(u.getCause());
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				future.completeExceptionally(e);
			}
		});
		return future;
	}

	public static void setGlobalFatalErrorHandler(@NotNull FatalErrorHandler handler) {
		globalFatalErrorHandler = handler;
	}

	// JMX
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, most of the durationStats are collected only when monitoring is enabled) ]")
	public void startExtendedMonitoring() {
		monitoring = true;
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, most of the durationStats are collected only when monitoring is enabled) ]")
	public void stopExtendedMonitoring() {
		monitoring = false;
	}

	@JmxAttribute(description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, most of the durationStats are collected only when monitoring is enabled)")
	public boolean isExtendedMonitoring() {
		return monitoring;
	}

	private void recordIoError(@NotNull Exception e, @Nullable Object context) {
		logger.warn("IO Error in {}", context, e);
	}

	private void onFatalError(@NotNull Throwable e, @Nullable Runnable runnable) {
		if (runnable instanceof RunnableWithContext) {
			recordFatalError(e, ((RunnableWithContext) runnable).getContext());
		} else {
			recordFatalError(e, runnable);
		}
	}

	public void recordFatalError(@NotNull Throwable e, @Nullable Object context) {
		while (e instanceof UncheckedException) {
			e = e.getCause();
		}
		logger.error("Fatal Error in {}", context, e);
		if (fatalErrorHandler != null) {
			handleFatalError(fatalErrorHandler, e, context);
		} else {
			handleFatalError(globalFatalErrorHandler, e, context);
		}
		if (inspector != null) {
			if (inEventloopThread()) {
				inspector.onFatalError(e, context);
			} else {
				Throwable finalE = e;
				execute(() -> inspector.onFatalError(finalE, context));
			}
		}
	}

	private void handleFatalError(@NotNull FatalErrorHandler handler, @NotNull Throwable e, @Nullable Object context) {
		if (inEventloopThread()) {
			handler.handle(e, context);
		} else {
			try {
				handler.handle(e, context);
			} catch (Throwable ignored) {
			}
		}
	}

	@JmxAttribute
	public int getLoop() {
		return loop;
	}

	@JmxAttribute
	public long getTick() {
		return tick;
	}

	@Nullable
	public FatalErrorHandler getFatalErrorHandler() {
		return fatalErrorHandler;
	}

	public int getThreadPriority() {
		return threadPriority;
	}

	@JmxAttribute
	public boolean getKeepAlive() {
		return keepAlive;
	}

	@Nullable
	@JmxAttribute(name = "")
	public EventloopStats getStats() {
		return BaseInspector.lookup(inspector, EventloopStats.class);
	}

	@JmxAttribute
	public Duration getIdleInterval() {
		return idleInterval;
	}

	@JmxAttribute
	public void setIdleInterval(Duration idleInterval) {
		this.idleInterval = idleInterval;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Eventloop");
		if (threadName != null) {
			sb.append('(').append(threadName).append(')');
		}
		sb.append("{loop=").append(loop);

		if (tick != 0) {
			sb.append(", tick=").append(tick);
		}
		if (selector != null && selector.isOpen()) {
			int selectorKeys = selector.keys().size() - cancelledKeys;
			if (selectorKeys != 0) {
				sb.append(", selectorKeys=").append(selectorKeys);
			}
		}
		if (!localTasks.isEmpty()) {
			sb.append(", localTasks=").append(localTasks.size());
		}
		if (!scheduledTasks.isEmpty()) {
			sb.append(", scheduledTasks=").append(scheduledTasks.size());
		}
		if (!backgroundTasks.isEmpty()) {
			sb.append(", backgroundTasks=").append(backgroundTasks.size());
		}
		if (!concurrentTasks.isEmpty()) {
			sb.append(", concurrentTasks=").append(concurrentTasks.size());
		}
		int externalTasks = externalTasksCount.get();
		if (externalTasks != 0) {
			sb.append(", externalTasks=").append(externalTasks);
		}
		return sb.append('}').toString();
	}
}
