package io.activej.cube.etcd;

import io.activej.async.service.ReactiveService;
import io.activej.common.ApplicationSettings;
import io.activej.common.Utils;
import io.activej.common.builder.AbstractBuilder;
import io.activej.cube.aggregation.ChunksAlreadyLockedException;
import io.activej.cube.aggregation.IChunkLocker;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.activej.etcd.exception.TransactionNotSucceededException;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.etcd.jetcd.*;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.VisibleForTesting;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.activej.common.Checks.checkArgument;
import static io.activej.etcd.EtcdUtils.executeTxnOps;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class EtcdChunkLocker<C> extends AbstractReactive
	implements IChunkLocker<C>, ReactiveService {

	public static final Duration DEFAULT_TTL = ApplicationSettings.getDuration(EtcdChunkLocker.class, "ttl", Duration.ofMinutes(10));

	private final Client client;
	private final ByteSequence root;
	private final EtcdKeyCodec<C> chunkCodec;

	private Duration ttl = DEFAULT_TTL;

	private final Map<Set<C>, Long> leaseIds = new HashMap<>();
	private final Map<Long, CloseableClient> keepAlives = new HashMap<>();

	private EtcdChunkLocker(Reactor reactor, Client client, ByteSequence root, EtcdKeyCodec<C> chunkCodec) {
		super(reactor);
		this.client = client;
		this.root = root;
		this.chunkCodec = chunkCodec;
	}

	public static <C> EtcdChunkLocker<C> create(Reactor reactor, Client client, ByteSequence root, EtcdKeyCodec<C> chunkCodec) {
		return EtcdChunkLocker.builder(reactor, client, root, chunkCodec).build();
	}

	public static <C> EtcdChunkLocker<C>.Builder builder(Reactor reactor, Client client, ByteSequence root, EtcdKeyCodec<C> chunkCodec) {
		return new EtcdChunkLocker<>(reactor, client, root, chunkCodec).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, EtcdChunkLocker<C>> {
		private Builder() {
		}

		public Builder withTTL(Duration ttl) {
			checkNotBuilt(this);
			EtcdChunkLocker.this.ttl = ttl;
			return this;
		}

		@Override
		protected EtcdChunkLocker<C> doBuild() {
			return EtcdChunkLocker.this;
		}
	}

	@Override
	public Promise<Void> lockChunks(Set<C> chunkIds) {
		checkInReactorThread(this);
		checkArgument(!chunkIds.isEmpty(), "Nothing to lock");

		return Promise.ofCompletionStage(client.getLeaseClient().grant(ttl.toSeconds()))
			.map(LeaseGrantResponse::getID)
			.then(leaseId -> Promise.ofCompletionStage(
					executeTxnOps(client.getKVClient(), root, txnOps -> {
						for (C chunkId : chunkIds) {
							ByteSequence key = chunkCodec.encodeKey(chunkId);
							txnOps.cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0));
							txnOps.put(key, ByteSequence.EMPTY,
								PutOption.builder()
									.withLeaseId(leaseId)
									.build());
						}
					}))
				.mapException(TransactionNotSucceededException.class, ChunksAlreadyLockedException::new)
				.whenResult($ -> {
					leaseIds.put(chunkIds, leaseId);
					keepAlives.put(leaseId, getKeepAlive(leaseId));
				}))
			.toVoid();
	}

	@Override
	public Promise<Void> releaseChunks(Set<C> chunkIds) {
		checkInReactorThread(this);
		checkArgument(!chunkIds.isEmpty(), "Nothing to release");

		Long leaseId = leaseIds.get(chunkIds);
		Lease leaseClient = client.getLeaseClient();
		if (leaseId != null) {
			return Promise.ofCompletionStage(leaseClient.revoke(leaseId))
				.whenComplete(() -> keepAlives.remove(leaseId).close())
				.toVoid();
		}

		Set<C> chunkIdsToRelease = new HashSet<>();
		Set<Long> releasedLeaseIds = new HashSet<>();

		for (Map.Entry<Set<C>, Long> entry : leaseIds.entrySet()) {
			Set<C> lockedIds = entry.getKey();
			Set<C> intersection = Utils.intersection(lockedIds, chunkIds);
			chunkIdsToRelease.addAll(intersection);
			if (intersection.size() == lockedIds.size()) {
				releasedLeaseIds.add(entry.getValue());
			}
		}

		return Promise.ofCompletionStage(
				executeTxnOps(client.getKVClient(), root, txnOps -> {
					for (C chunkId : chunkIdsToRelease) {
						ByteSequence key = chunkCodec.encodeKey(chunkId);
						txnOps.delete(key, DeleteOption.DEFAULT);
					}
				}))
			.whenResult(() -> {
				for (Long releasedLeaseId : releasedLeaseIds) {
					CloseableClient closeableClient = keepAlives.remove(releasedLeaseId);
					if (closeableClient != null) closeableClient.close();
					leaseClient.revoke(releasedLeaseId);
				}
			})
			.toVoid();
	}

	@Override
	public Promise<Set<C>> getLockedChunks() {
		return Promise.ofCompletionStage(client.getKVClient().get(root,
				GetOption.builder()
					.isPrefix(true)
					.build()))
			.map(getResponse -> {
				Set<C> lockedChunks = new HashSet<>();
				for (KeyValue kv : getResponse.getKvs()) {
					ByteSequence key = kv.getKey().substring(root.size());
					C chunkId;
					try {
						chunkId = chunkCodec.decodeKey(key);
					} catch (MalformedEtcdDataException e) {
						throw new MalformedEtcdDataException("Failed to decode key '" + key + '\'', e);
					}
					lockedChunks.add(chunkId);
				}
				return lockedChunks;
			});
	}

	@Override
	public Promise<?> start() {
		return Promise.complete();
	}

	@Override
	public Promise<?> stop() {
		Map<Long, CloseableClient> keepAlivesCopy = Map.copyOf(keepAlives);
		keepAlives.clear();

		for (CloseableClient keepAliveClient : keepAlivesCopy.values()) {
			keepAliveClient.close();
		}

		Map<Set<C>, Long> leaseIdsCopy = Map.copyOf(leaseIds);
		leaseIds.clear();

		Lease leaseClient = client.getLeaseClient();
		return Promises.all(leaseIdsCopy.values().stream()
			.map(leaseId -> Promise.ofCompletionStage(leaseClient.revoke(leaseId))));
	}

	private CloseableClient getKeepAlive(Long leaseId) {
		return client.getLeaseClient().keepAlive(leaseId, new StreamObserver<>() {
			@Override
			public void onNext(LeaseKeepAliveResponse leaseKeepAliveResponse) {
			}

			@Override
			public void onError(Throwable throwable) {
				CloseableClient closeableClient = keepAlives.remove(leaseId);
				if (closeableClient != null) closeableClient.close();
			}

			@Override
			public void onCompleted() {
				CloseableClient closeableClient = keepAlives.remove(leaseId);
				if (closeableClient != null) closeableClient.close();
			}
		});
	}

	@VisibleForTesting
	public void delete() throws ExecutionException, InterruptedException {
		KV kvClient = client.getKVClient();
		kvClient
			.delete(root,
				DeleteOption.builder()
					.isPrefix(true)
					.build())
			.get();
	}
}
