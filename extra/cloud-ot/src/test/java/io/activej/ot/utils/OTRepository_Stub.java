package io.activej.ot.utils;

import io.activej.ot.AsyncOTCommitFactory;
import io.activej.ot.OTCommit;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.promise.Promise;
import io.activej.reactor.ImplicitlyReactive;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Utils.not;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.toSet;

public final class OTRepository_Stub<K, D> extends ImplicitlyReactive
		implements AsyncOTRepository<K, D> {
	public Supplier<K> revisionIdSupplier;
	private AsyncOTCommitFactory<K, D> commitFactory;

	public final Map<K, OTCommit<K, D>> commits = new LinkedHashMap<>();
	public final Set<K> heads = new HashSet<>();
	public final Map<K, List<D>> snapshots = new LinkedHashMap<>();

	private OTRepository_Stub(Supplier<K> revisionIdSupplier) {
		this.revisionIdSupplier = revisionIdSupplier;
	}

	public static <K, D> OTRepository_Stub<K, D> create() {
		return new OTRepository_Stub<>(null);
	}

	public static <K, D> OTRepository_Stub<K, D> create(Stream<K> newIds) {
		return create(newIds.iterator());
	}

	public static <K, D> OTRepository_Stub<K, D> create(Iterable<K> newIds) {
		return create(newIds.iterator());
	}

	public static <K, D> OTRepository_Stub<K, D> create(Iterator<K> newIds) {
		return new OTRepository_Stub<>(newIds::next);
	}

	public void setCommitFactory(AsyncOTCommitFactory<K, D> commitFactory) {
		this.commitFactory = commitFactory;
	}

	public void setGraph(Consumer<OTGraphBuilder<K, D>> builder) {
		commits.clear();
		List<OTCommit<K, D>> commits = Utils.commits(builder);
		doPushAndUpdateHeads(commits);
		for (OTCommit<K, D> commit : commits) {
			if (commit.getLevel() == 1L) {
				doSaveSnapshot(commit.getId(), List.of());
			}
		}
	}

	public void addGraph(Consumer<OTGraphBuilder<K, D>> builder) {
		long initialLevel = 1L + commits.values().stream()
				.mapToLong(OTCommit::getLevel)
				.max()
				.orElse(0L);
		List<OTCommit<K, D>> commits = Utils.commits(builder, false, initialLevel);
		doPushAndUpdateHeads(commits);
	}

	public Promise<K> createCommitId() {
		return Promise.of(doCreateCommitId());
	}

	@Override
	public Promise<OTCommit<K, D>> createCommit(Map<K, DiffsWithLevel<D>> parentDiffs) {
		checkInReactorThread(this);
		return commitFactory != null ?
				commitFactory.createCommit(parentDiffs) :
				createCommitId()
						.map(newId -> OTCommit.of(0, newId, parentDiffs));
	}

	@Override
	public Promise<Void> push(Collection<OTCommit<K, D>> commits) {
		checkInReactorThread(this);
		doPush(commits);
		return Promise.complete();
	}

	@Override
	public Promise<Void> updateHeads(Set<K> newHeads, Set<K> excludedHeads) {
		checkInReactorThread(this);
		heads.addAll(newHeads);
		heads.removeAll(excludedHeads);
		return Promise.complete();
	}

	@Override
	public Promise<Set<K>> getAllHeads() {
		checkInReactorThread(this);
		return Promise.of(new HashSet<>(heads));
	}

	public K doCreateCommitId() {
		return revisionIdSupplier.get();
	}

	@Override
	public Promise<Boolean> hasCommit(K revisionId) {
		checkInReactorThread(this);
		return Promise.of(commits.containsKey(revisionId));
	}

	@Override
	public Promise<OTCommit<K, D>> loadCommit(K revisionId) {
		checkInReactorThread(this);
		return Promise.of(doLoadCommit(revisionId));
	}

	@Override
	public Promise<Void> saveSnapshot(K revisionId, List<D> diffs) {
		checkInReactorThread(this);
		doSaveSnapshot(revisionId, diffs);
		return Promise.complete();
	}

	@Override
	public Promise<Optional<List<D>>> loadSnapshot(K revisionId) {
		checkInReactorThread(this);
		return Promise.of(Optional.ofNullable(snapshots.get(revisionId)));
	}

	public void doPush(OTCommit<K, D> commit) {
		commits.put(commit.getId(), commit);
	}

	public void doPushAndUpdateHead(OTCommit<K, D> commit) {
		commits.put(commit.getId(), commit);
		heads.add(commit.getId());
		heads.removeAll(commit.getParentIds());
	}

	public void doPush(Collection<OTCommit<K, D>> commits) {
		for (OTCommit<K, D> commit : commits) {
			doPush(commit);
		}
	}

	public void doPushAndUpdateHeads(Collection<OTCommit<K, D>> commits) {
		for (OTCommit<K, D> commit : commits) {
			doPush(commit);
		}
		Set<K> parents = commits.stream()
				.flatMap(commit -> commit.getParents().keySet().stream())
				.collect(toSet());
		Set<K> heads = commits.stream()
				.map(OTCommit::getId)
				.filter(not(parents::contains))
				.collect(toSet());
		updateHeads(heads, parents);
	}

	public OTCommit<K, D> doLoadCommit(K revisionId) {
		OTCommit<K, D> commit = commits.get(revisionId);
		checkNotNull(commit);
		return OTCommit.builder(0, commit.getId(), commit.getParentsWithLevels())
				.withTimestamp(commit.getTimestamp())
				.build();
	}

	public void doSaveSnapshot(K revisionId, List<D> diffs) {
		snapshots.put(revisionId, diffs);
	}

	public void reset() {
		commits.clear();
		heads.clear();
		snapshots.clear();
	}

}
