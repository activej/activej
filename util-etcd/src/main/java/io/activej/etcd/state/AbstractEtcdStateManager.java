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
import io.etcd.jetcd.options.DeleteOption;
import org.jetbrains.annotations.NotNull;

import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static io.activej.etcd.EtcdUtils.executeTxnOps;

public abstract class AbstractEtcdStateManager<S, T> implements BlockingService {
	private final Client client;
	private final ByteSequence root;

	private final EtcdUtils.CheckoutRequest<?, ?>[] checkoutRequests;
	private final EtcdUtils.WatchRequest<?, ?, ?>[] watchRequests;

	private volatile Watch.Watcher watcher;

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
		doCheckout().get();
		doWatch();
	}

	@Override
	public void stop() {
		if (this.watcher != null) {
			this.watcher.close();
		}
	}

	private CompletableFuture<Void> doCheckout() {
		return EtcdUtils.checkout(client, revision, checkoutRequests,
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

	private void doWatch() {
		this.watcher = EtcdUtils.watch(client, revision + 1L, watchRequests,
			new EtcdListener<>() {
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
				}

				@Override
				public void onCompleted() {
				}
			});
	}

	protected abstract void applyStateTransitions(S state, Object[] operation) throws MalformedEtcdDataException;

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

	public void delete() throws ExecutionException, InterruptedException {
		client.getKVClient()
			.delete(root,
				DeleteOption.builder()
					.isPrefix(true)
					.build())
			.get();
		client.getKVClient()
			.put(root, ByteSequence.EMPTY)
			.get();
	}

}
