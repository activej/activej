package io.activej.etcd.state;

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

import static io.activej.etcd.EtcdUtils.executeTxnOps;

public abstract class AbstractEtcdStateManager<S, T> {
	private Client client;
	private ByteSequence root;

	private final EtcdUtils.CheckoutRequest<?, ?>[] checkoutRequests;
	private final EtcdUtils.WatchRequest<?, ?, ?>[] watchRequests;

	private volatile Watch.Watcher watcher;

	private volatile long revision;
	private S state;

	protected final ReadWriteLock stateLock = new ReentrantReadWriteLock();

	protected AbstractEtcdStateManager(EtcdUtils.CheckoutRequest<?, ?>[] checkoutRequests, EtcdUtils.WatchRequest<?, ?, ?>[] watchRequests) {
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

	private CompletableFuture<Void> doCheckout(long revision) {
		return EtcdUtils.checkout(client, revision, checkoutRequests,
			(header, objects) -> {
				stateLock.writeLock().lock();
				try {
					this.revision = header.getRevision();
					this.state = finishState(header, objects);
				} finally {
					stateLock.writeLock().unlock();
				}

				synchronized (promisesQueue) {
					processQueue(header.getRevision());
				}

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

	private void doWatch(long revision) {
		this.watcher = EtcdUtils.watch(client, revision + 1L, watchRequests,
			new EtcdListener<Object[]>() {
				@Override
				public void onNext(long revision, Object[] operation) throws MalformedEtcdDataException {
					stateLock.writeLock().lock();
					try {
						applyStateTransitions(operation);
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
				}

				@Override
				public void onCompleted() {
				}
			});
	}

	protected abstract void applyStateTransitions(Object[] operation) throws MalformedEtcdDataException;

	public CompletableFuture<Response.Header> push(T transaction) {
		CompletableFuture<Response.Header> result = new CompletableFuture<>();

		executeTxnOps(client, root, txnOps -> doPush(txnOps, transaction))
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

	void processQueue(long currentRevision) {
		synchronized (promisesQueue) {
			while (true) {
				PromiseEntry peeked = promisesQueue.peek();
				if (peeked == null || peeked.revision > currentRevision) break;
				promisesQueue.poll();
				peeked.future.complete(peeked.header);
			}
		}
	}

}
