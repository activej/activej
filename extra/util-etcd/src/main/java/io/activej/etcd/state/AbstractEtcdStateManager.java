package io.activej.etcd.state;

import io.activej.common.service.BlockingService;
import io.activej.etcd.EtcdListener;
import io.activej.etcd.EtcdUtils;
import io.activej.etcd.TxnOps;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Response;
import io.etcd.jetcd.Watch;
import org.jetbrains.annotations.NotNull;

import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static io.activej.common.Checks.checkState;
import static io.activej.etcd.EtcdUtils.executeTxnOps;

public abstract class AbstractEtcdStateManager<S, T> implements BlockingService {
	protected final Client client;
	protected final ByteSequence root;

	private final EtcdUtils.CheckoutRequest<?, ?>[] checkoutRequests;
	private final EtcdUtils.WatchRequest<?, ?, ?>[] watchRequests;

	protected volatile Watch.Watcher watcher;

	private volatile long revision;
	private S state;

	protected final ReadWriteLock stateLock = new ReentrantReadWriteLock();

	protected AbstractEtcdStateManager(Client client, ByteSequence root, EtcdUtils.CheckoutRequest<?, ?>[] checkoutRequests, EtcdUtils.WatchRequest<?, ?, ?>[] watchRequests) {
		this.client = client;
		this.root = root;
		this.checkoutRequests = checkoutRequests;
		this.watchRequests = watchRequests;
	}

	private record PromiseEntry(long revision, Response.Header header, CompletableFuture<Response.Header> future) implements Comparable<PromiseEntry> {
		@Override
		public int compareTo(@NotNull PromiseEntry other) {
			return Long.compare(this.revision, other.revision);
		}
	}

	private final PriorityQueue<PromiseEntry> promisesQueue = new PriorityQueue<>();

	@Override
	public void start() throws Exception {
		checkState(state == null, "Already started");
		doCheckout().get();
		watch();
	}

	@Override
	public void stop() {
		if (this.watcher != null) {
			this.watcher.close();
		}
	}

	private CompletableFuture<Void> doCheckout() {
		return EtcdUtils.checkout(client.getKVClient(), revision, checkoutRequests,
			(header, objects) -> {
				this.revision = header.getRevision();
				this.state = finishState(header, objects);
				return null;
			}
		);
	}

	public final <R> R query(Function<S, R> query) {
		stateLock.readLock().lock();
		try {
			return query.apply(state);
		} finally {
			stateLock.readLock().unlock();
		}
	}

	protected abstract S finishState(Response.Header header, Object[] checkoutObjects) throws MalformedEtcdDataException;

	protected void onWatchConnectionEstablished() {
	}

	protected void onWatchError(Throwable throwable) {
	}

	protected void onWatchCompleted() {
	}

	protected final void watch() {
		Watch.Watcher watcher = this.watcher;
		this.watcher = null;
		if (watcher != null) {
			watcher.close();
		}
		this.watcher = doCreateWatcher();
	}

	private Watch.Watcher doCreateWatcher() {
		StateListener listener = new StateListener();
		Watch.Watcher watcher = EtcdUtils.watch(client.getWatchClient(), revision + 1L, watchRequests, listener);
		listener.setWatcher(watcher);
		return watcher;
	}

	protected abstract void applyStateTransitions(S state, Object[] operation) throws MalformedEtcdDataException;

	public CompletableFuture<Response.Header> push(T transaction) {
		CompletableFuture<Response.Header> result = new CompletableFuture<>();

		executeTxnOps(client.getKVClient(), root, txnOps -> doPush(txnOps, transaction))
			.whenComplete((txnResponse, throwable) -> {
				if (throwable != null) {
					result.completeExceptionally(throwable);
					return;
				}

				long revision = this.revision;

				if (revision >= txnResponse.getHeader().getRevision()) {
					result.complete(txnResponse.getHeader());
					return;
				}

				synchronized (promisesQueue) {
					promisesQueue.add(new PromiseEntry(txnResponse.getHeader().getRevision(), txnResponse.getHeader(), result));
				}
			});

		return result;
	}

	protected abstract void doPush(TxnOps txn, T transaction);

	private void processQueue(long currentRevision) {
		synchronized (promisesQueue) {
			while (true) {
				PromiseEntry peeked = promisesQueue.peek();
				if (peeked == null || peeked.revision > currentRevision) break;
				promisesQueue.poll();
				peeked.future.complete(peeked.header);
			}
		}
	}

	public long getRevision() {
		return revision;
	}

	private final class StateListener implements EtcdListener<Object[]> {
		private volatile boolean closeWatcher;
		private volatile Watch.Watcher watcher;

		@Override
		public void onConnectionEstablished() {
			AbstractEtcdStateManager.this.onWatchConnectionEstablished();
		}

		@Override
		public void onNext(long revision, Object[] operation) throws MalformedEtcdDataException {
			stateLock.writeLock().lock();
			try {
				applyStateTransitions(state, operation);
				AbstractEtcdStateManager.this.revision = revision;
			} finally {
				stateLock.writeLock().unlock();
			}

			synchronized (promisesQueue) {
				processQueue(revision);
			}
		}

		@Override
		public void onError(Throwable throwable) {
			AbstractEtcdStateManager.this.onWatchError(throwable);
			closeWatcher();
		}

		@Override
		public void onCompleted() {
			AbstractEtcdStateManager.this.onWatchCompleted();
			closeWatcher();
		}

		void closeWatcher() {
			closeWatcher = true;
			if (watcher != null) {
				watcher.close();
			}
		}

		void setWatcher(Watch.Watcher watcher) {
			this.watcher = watcher;
			if (closeWatcher) {
				watcher.close();
			}
		}
	}
}
