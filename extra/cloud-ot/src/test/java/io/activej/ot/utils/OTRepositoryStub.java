package io.activej.ot.utils;

import io.activej.ot.OTCommit;
import io.activej.ot.OTCommitFactory;
import io.activej.ot.repository.OTRepository;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Utils.not;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;

public final class OTRepositoryStub<K, D> implements OTRepository<K, D> {
	public Supplier<K> revisionIdSupplier;
	private OTCommitFactory<K, D> commitFactory;

	public final Map<K, OTCommit<K, D>> commits = new LinkedHashMap<>();
	public final Set<K> heads = new HashSet<>();
	public final Map<K, List<D>> snapshots = new LinkedHashMap<>();

	private OTRepositoryStub(Supplier<K> revisionIdSupplier) {
		this.revisionIdSupplier = revisionIdSupplier;
	}

	public static <K, D> OTRepositoryStub<K, D> create() {
		return new OTRepositoryStub<>(null);
	}

	public static <K, D> OTRepositoryStub<K, D> create(Stream<K> newIds) {
		return create(newIds.iterator());
	}

	public static <K, D> OTRepositoryStub<K, D> create(Iterable<K> newIds) {
		return create(newIds.iterator());
	}

	public static <K, D> OTRepositoryStub<K, D> create(Iterator<K> newIds) {
		return new OTRepositoryStub<>(newIds::next);
	}

	public void setCommitFactory(OTCommitFactory<K, D> commitFactory) {
		this.commitFactory = commitFactory;
	}

	public void setGraph(Consumer<OTGraphBuilder<K, D>> builder) {
		commits.clear();
		List<OTCommit<K, D>> commits = Utils.commits(builder);
		doPushAndUpdateHeads(commits);
		for (OTCommit<K, D> commit : commits) {
			if (commit.getLevel() == 1L) {
				doSaveSnapshot(commit.getId(), emptyList());
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
		return commitFactory != null ?
				commitFactory.createCommit(parentDiffs) :
				createCommitId()
						.map(newId -> OTCommit.of(0, newId, parentDiffs));
	}

	@Override
	public Promise<Void> push(Collection<OTCommit<K, D>> commits) {
		doPush(commits);
		return Promise.complete();
	}

	@Override
	public @NotNull Promise<Void> updateHeads(Set<K> newHeads, Set<K> excludedHeads) {
		heads.addAll(newHeads);
		heads.removeAll(excludedHeads);
		return Promise.complete();
	}

	@Override
	public @NotNull Promise<Set<K>> getAllHeads() {
		return Promise.of(new HashSet<>(heads));
	}

	public K doCreateCommitId() {
		return revisionIdSupplier.get();
	}

	@Override
	public @NotNull Promise<Boolean> hasCommit(@NotNull K revisionId) {
		return Promise.of(commits.containsKey(revisionId));
	}

	@Override
	public @NotNull Promise<OTCommit<K, D>> loadCommit(@NotNull K revisionId) {
		return Promise.of(doLoadCommit(revisionId));
	}

	@Override
	public @NotNull Promise<Void> saveSnapshot(@NotNull K revisionId, @NotNull List<D> diffs) {
		doSaveSnapshot(revisionId, diffs);
		return Promise.complete();
	}

	@Override
	public @NotNull Promise<Optional<List<D>>> loadSnapshot(@NotNull K revisionId) {
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
		return OTCommit.of(0, commit.getId(), commit.getParentsWithLevels())
				.withTimestamp(commit.getTimestamp());
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
