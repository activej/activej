package io.activej.cube.etcd;

import io.activej.async.service.ReactiveService;
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.collection.CollectionUtils;
import io.activej.cube.aggregation.ChunksAlreadyLockedException;
import io.activej.cube.aggregation.IChunkLocker;
import io.activej.etcd.EtcdUtils;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.activej.etcd.codec.key.EtcdKeyCodecs;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.activej.etcd.exception.TransactionNotSucceededException;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.etcd.jetcd.*;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
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

public final class EtcdChunkLocker extends AbstractReactive
	implements IChunkLocker, ReactiveService {

	private static final EtcdKeyCodec<Long> CHUNK_ID_CODEC = EtcdKeyCodecs.ofLong();

	public static final Duration DEFAULT_TTL = ApplicationSettings.getDuration(EtcdChunkLocker.class, "ttl", Duration.ofMinutes(10));

	private final Client client;
	private final ByteSequence root;

	private Duration ttl = DEFAULT_TTL;

	private final Map<Set<Long>, Long> leaseIds = new HashMap<>();

	private EtcdChunkLocker(Reactor reactor, Client client, ByteSequence root) {
		super(reactor);
		this.client = client;
		this.root = root;
	}

	public static EtcdChunkLocker create(Reactor reactor, Client client, ByteSequence root) {
		return EtcdChunkLocker.builder(reactor, client, root).build();
	}

	public static EtcdChunkLocker.Builder builder(Reactor reactor, Client client, ByteSequence root) {
		return new EtcdChunkLocker(reactor, client, root).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, EtcdChunkLocker> {
		private Builder() {
		}

		public Builder withTTL(Duration ttl) {
			checkNotBuilt(this);
			EtcdChunkLocker.this.ttl = ttl;
			return this;
		}

		@Override
		protected EtcdChunkLocker doBuild() {
			return EtcdChunkLocker.this;
		}
	}

	@Override
	public Promise<Void> lockChunks(Set<Long> chunkIds) {
		checkInReactorThread(this);
		checkArgument(!chunkIds.isEmpty(), "Nothing to lock");

		return Promise.ofCompletionStage(client.getLeaseClient().grant(ttl.toSeconds())
				.exceptionallyCompose(EtcdUtils::convertStatusExceptionStage))
			.map(LeaseGrantResponse::getID)
			.then(leaseId -> Promise.ofCompletionStage(
					executeTxnOps(client.getKVClient(), root, txnOps -> {
						for (long chunkId : chunkIds) {
							ByteSequence key = CHUNK_ID_CODEC.encodeKey(chunkId);
							txnOps.cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0));
							txnOps.put(key, ByteSequence.EMPTY,
								PutOption.builder()
									.withLeaseId(leaseId)
									.build());
						}
					}))
				.mapException(TransactionNotSucceededException.class, ChunksAlreadyLockedException::new)
				.whenResult($ -> leaseIds.put(chunkIds, leaseId)))
			.toVoid();
	}

	@Override
	public Promise<Void> releaseChunks(Set<Long> chunkIds) {
		checkInReactorThread(this);
		checkArgument(!chunkIds.isEmpty(), "Nothing to release");

		Long leaseId = leaseIds.get(chunkIds);
		Lease leaseClient = client.getLeaseClient();
		if (leaseId != null) {
			return Promise.ofCompletionStage(leaseClient.revoke(leaseId)
					.exceptionallyCompose(EtcdUtils::convertStatusExceptionStage))
				.toVoid();
		}

		Set<Long> chunkIdsToRelease = new HashSet<>();
		Set<Long> releasedLeaseIds = new HashSet<>();

		for (Map.Entry<Set<Long>, Long> entry : leaseIds.entrySet()) {
			Set<Long> lockedIds = entry.getKey();
			Set<Long> intersection = CollectionUtils.intersection(lockedIds, chunkIds);
			chunkIdsToRelease.addAll(intersection);
			if (intersection.size() == lockedIds.size()) {
				releasedLeaseIds.add(entry.getValue());
			}
		}

		return Promise.ofCompletionStage(
				executeTxnOps(client.getKVClient(), root, txnOps -> {
					for (long chunkId : chunkIdsToRelease) {
						ByteSequence key = CHUNK_ID_CODEC.encodeKey(chunkId);
						txnOps.delete(key, DeleteOption.DEFAULT);
					}
				}))
			.whenResult(() -> {
				for (Long releasedLeaseId : releasedLeaseIds) {
					leaseClient.revoke(releasedLeaseId);
				}
			})
			.toVoid();
	}

	@Override
	public Promise<Set<Long>> getLockedChunks() {
		return Promise.ofCompletionStage(client.getKVClient().get(root,
					GetOption.builder()
						.isPrefix(true)
						.build())
				.exceptionallyCompose(EtcdUtils::convertStatusExceptionStage))
			.map(getResponse -> {
				Set<Long> lockedChunks = new HashSet<>();
				for (KeyValue kv : getResponse.getKvs()) {
					ByteSequence key = kv.getKey().substring(root.size());
					long chunkId;
					try {
						chunkId = CHUNK_ID_CODEC.decodeKey(key);
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
		Map<Set<Long>, Long> leaseIdsCopy = Map.copyOf(leaseIds);
		leaseIds.clear();

		Lease leaseClient = client.getLeaseClient();
		return Promises.all(leaseIdsCopy.values().stream()
			.map(leaseId -> Promise.ofCompletionStage(leaseClient.revoke(leaseId)
				.exceptionallyCompose(EtcdUtils::convertStatusExceptionStage))));
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
