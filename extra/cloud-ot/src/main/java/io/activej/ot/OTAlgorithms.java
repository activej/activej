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

package io.activej.ot;

import io.activej.async.function.AsyncPredicate;
import io.activej.common.ref.Ref;
import io.activej.ot.AsyncOTCommitFactory.DiffsWithLevel;
import io.activej.ot.exception.GraphExhaustedException;
import io.activej.ot.exception.OTException;
import io.activej.ot.reducers.AbstractGraphReducer;
import io.activej.ot.reducers.DiffsReducer;
import io.activej.ot.reducers.GraphReducer;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.*;
import static io.activej.ot.reducers.GraphReducer.Result.*;
import static io.activej.promise.Promises.toList;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toSet;

public final class OTAlgorithms {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);

	public static <K, D, R> Promise<R> reduce(AsyncOTRepository<K, D> repository, OTSystem<D> system,
			Set<K> heads, GraphReducer<K, D, R> reducer) {
		return toList(heads.stream().map(repository::loadCommit))
				.then(headCommits -> {
					PriorityQueue<OTCommit<K, D>> queue = new PriorityQueue<>(reverseOrder(comparingLong(OTCommit::getLevel)));
					queue.addAll(headCommits);
					reducer.onStart(unmodifiableCollection(queue));
					return Promise.ofCallback(cb -> walkGraphImpl(repository, reducer, queue, new HashSet<>(heads), cb));
				});
	}

	private static <K, D, R> void walkGraphImpl(AsyncOTRepository<K, D> repository, GraphReducer<K, D, R> reducer,
			PriorityQueue<OTCommit<K, D>> queue, Set<K> visited, SettablePromise<R> cb) {
		OTCommit<K, D> commit = queue.peek();
		if (commit == null) {
			cb.setException(new GraphExhaustedException());
			return;
		}
		reducer.onCommit(commit)
				.run((maybeResult, e) -> {
					if (e != null) {
						cb.setException(e);
						return;
					}
					OTCommit<K, D> polledCommit = queue.poll();
					assert polledCommit == commit;
					if (maybeResult.isResume()) {
						toList(commit.getParents().keySet().stream().filter(visited::add).map(repository::loadCommit))
								.async()
								.run((parentCommits, e2) -> {
									if (e2 == null) {
										queue.addAll(parentCommits);
										walkGraphImpl(repository, reducer, queue, visited, cb);
									} else {
										cb.setException(e2);
									}
								});
					} else if (maybeResult.isSkip()) {
						walkGraphImpl(repository, reducer, queue, visited, cb);
					} else {
						cb.set(maybeResult.get());
					}
				});
	}

	public static final class FindResult<K, A> {
		private final int epoch;
		private final K commit;
		private final Set<K> commitParents;
		private final long commitLevel;
		private final K child;
		private final long childLevel;
		private final A accumulatedDiffs;

		private FindResult(int epoch, K commit, Set<K> commitParents, long commitLevel, K child, long childLevel, A accumulatedDiffs) {
			this.epoch = epoch;
			this.commit = commit;
			this.commitParents = commitParents;
			this.commitLevel = commitLevel;
			this.child = child;
			this.childLevel = childLevel;
			this.accumulatedDiffs = accumulatedDiffs;
		}

		public int getEpoch() {
			return epoch;
		}

		public K getCommit() {
			return commit;
		}

		public K getChild() {
			return child;
		}

		public Long getChildLevel() {
			return childLevel;
		}

		public Set<K> getCommitParents() {
			return commitParents;
		}

		public long getCommitLevel() {
			return commitLevel;
		}

		public A getAccumulatedDiffs() {
			return accumulatedDiffs;
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"commit=" + commit +
					", parents=" + commitParents +
					", child=" + child +
					", accumulator=" + accumulatedDiffs +
					'}';
		}
	}

	public static <K, D, A> Promise<FindResult<K, A>> findParent(AsyncOTRepository<K, D> repository, OTSystem<D> system,
			Set<K> startNodes, DiffsReducer<A, D> diffsReducer, AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		return reduce(repository, system, startNodes,
				new AbstractGraphReducer<>(diffsReducer) {
					int epoch;

					@Override
					public void onStart(Collection<OTCommit<K, D>> queue) {
						this.epoch = queue.iterator().next().getEpoch();
						super.onStart(queue);
					}

					@Override
					protected Promise<Optional<FindResult<K, A>>> tryGetResult(OTCommit<K, D> commit,
							Map<K, Map<K, A>> accumulators, Map<K, OTCommit<K, D>> headCommits) {
						return matchPredicate.test(commit)
								.mapIfElse(matched -> matched,
										$ -> {
											Map.Entry<K, A> someHead = accumulators.get(commit.getId()).entrySet().iterator().next();
											return Optional.of(new FindResult<>(
													epoch, commit.getId(), commit.getParentIds(), commit.getLevel(),
													someHead.getKey(), headCommits.get(someHead.getKey()).getLevel(),
													someHead.getValue()
											));
										},
										$ -> Optional.empty());
					}
				});
	}

	public static <K, D> Promise<K> mergeAndPush(AsyncOTRepository<K, D> repository, OTSystem<D> system) {
		return repository.getHeads()
				.then(heads -> mergeAndPush(repository, system, heads))
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public static <K, D> Promise<K> mergeAndPush(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads) {
		if (heads.size() == 1) return Promise.of(first(heads)); // nothing to merge
		return merge(repository, system, heads)
				.then(mergeCommit -> repository.push(mergeCommit)
						.map($ -> mergeCommit.getId()))
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public static <K, D> Promise<K> mergeAndUpdateHeads(AsyncOTRepository<K, D> repository, OTSystem<D> system) {
		return repository.getHeads()
				.then(heads -> mergeAndUpdateHeads(repository, system, heads));
	}

	public static <K, D> Promise<K> mergeAndUpdateHeads(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads) {
		return mergeAndPush(repository, system, heads)
				.then(mergeId -> repository.updateHeads(difference(Set.of(mergeId), heads), difference(heads, Set.of(mergeId)))
						.map($ -> mergeId))
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public static <K, D> Promise<OTCommit<K, D>> merge(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads) {
		checkArgument(heads.size() >= 2, "Cannot merge less than 2 heads");
		return repository.getLevels(heads)
				.then(levels ->
						reduce(repository, system, heads, new LoadGraphReducer<>(system))
								.map(graph -> {
									try {
										Map<K, List<D>> mergeResult = graph.merge(graph.excludeParents(heads));
										if (logger.isTraceEnabled()) {
											logger.info("{}\n", graph.toGraphViz());
										}
										return mergeResult;
									} catch (OTException e) {
										if (logger.isTraceEnabled()) {
											logger.error("{}\n", graph.toGraphViz(), e);
										}
										throw e;
									}
								})
								.then(mergeResult -> repository.createCommit(
										keysToMap(heads.stream(), head -> new DiffsWithLevel<>(levels.get(head), mergeResult.get(head))))));
	}

	public static <K, D> Promise<Set<K>> findCut(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> startNodes,
			Predicate<Collection<OTCommit<K, D>>> matchPredicate) {
		return reduce(repository, system, startNodes,
				new GraphReducer<>() {
					private Collection<OTCommit<K, D>> queue;

					@Override
					public void onStart(Collection<OTCommit<K, D>> queue) {
						this.queue = queue;
					}

					@Override
					public Promise<Result<Set<K>>> onCommit(OTCommit<K, D> commit) {
						if (matchPredicate.test(queue)) {
							return completePromise(queue.stream().map(OTCommit::getId).collect(toSet()));
						}
						return resumePromise();
					}
				});
	}

	public static <K, D> Promise<K> findAnyCommonParent(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> startCut) {
		return reduce(repository, system, startCut, new FindAnyCommonParentReducer<>(DiffsReducer.toVoid()))
				.map(Map.Entry::getKey)
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public static <K, D> Promise<Set<K>> findAllCommonParents(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> startCut) {
		return reduce(repository, system, startCut, new FindAllCommonParentsReducer<>(DiffsReducer.toVoid()))
				.map(Map::keySet)
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public static <K, D> Promise<List<D>> diff(AsyncOTRepository<K, D> repository, OTSystem<D> system, K node1, K node2) {
		if (node1.equals(node2)) return Promise.of(List.of());

		Set<K> startCut = Set.of(node1, node2);
		return reduce(repository, system, startCut, new FindAnyCommonParentReducer<>(DiffsReducer.toList()))
				.map(entry -> {
					List<D> diffs1 = entry.getValue().get(node1);
					List<D> diffs2 = entry.getValue().get(node2);
					return concat(diffs2, system.invert(diffs1));
				})
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public static <K, D> Promise<Set<K>> excludeParents(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> startNodes) {
		checkArgument(!startNodes.isEmpty(), "Start nodes are empty");
		if (startNodes.size() == 1) return Promise.of(startNodes);
		return reduce(repository, system, startNodes,
				new GraphReducer<K, D, Set<K>>() {
					long minLevel;
					final Set<K> nodes = new HashSet<>(startNodes);

					@Override
					public void onStart(Collection<OTCommit<K, D>> queue) {
						//noinspection OptionalGetWithoutIsPresent
						minLevel = queue.stream().mapToLong(OTCommit::getLevel).min().getAsLong();
					}

					@Override
					public Promise<Result<Set<K>>> onCommit(OTCommit<K, D> commit) {
						nodes.removeAll(commit.getParentIds());
						if (commit.getLevel() <= minLevel) {
							return completePromise(nodes);
						}
						return resumePromise();
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), startNodes));
	}

	private static final class FindAnyCommonParentReducer<K, D, A> extends AbstractGraphReducer<K, D, A, Map.Entry<K, Map<K, A>>> {
		private FindAnyCommonParentReducer(DiffsReducer<A, D> diffsReducer) {
			super(diffsReducer);
		}

		@Override
		protected Promise<Optional<Map.Entry<K, Map<K, A>>>> tryGetResult(OTCommit<K, D> commit,
				Map<K, Map<K, A>> accumulators, Map<K, OTCommit<K, D>> headCommits) {
			return Promise.of(accumulators.entrySet()
					.stream()
					.filter(entry -> Objects.equals(headCommits.keySet(), entry.getValue().keySet()))
					.findAny()
			);
		}
	}

	private static final class FindAllCommonParentsReducer<K, D, A> extends AbstractGraphReducer<K, D, A, Map<K, Map<K, A>>> {
		private FindAllCommonParentsReducer(DiffsReducer<A, D> diffsReducer) {
			super(diffsReducer);
		}

		@Override
		protected Promise<Optional<Map<K, Map<K, A>>>> tryGetResult(OTCommit<K, D> commit, Map<K, Map<K, A>> accumulators,
				Map<K, OTCommit<K, D>> headCommits) {
			return Promise.of(
					accumulators.values()
							.stream()
							.map(Map::keySet)
							.allMatch(headCommits.keySet()::equals) ? Optional.of(accumulators) : Optional.empty()
			);
		}
	}

	public static <K, D, A> Promise<Map<K, A>> reduceEdges(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads, K parentNode,
			DiffsReducer<A, D> diffAccumulator) {
		return reduce(repository, system, heads, new AbstractGraphReducer<>(diffAccumulator) {
			@Override
			protected Promise<Optional<Map<K, A>>> tryGetResult(OTCommit<K, D> commit, Map<K, Map<K, A>> accumulators, Map<K, OTCommit<K, D>> headCommits) {
				if (accumulators.containsKey(parentNode)) {
					Map<K, A> toHeads = accumulators.get(parentNode);
					if (Objects.equals(heads, toHeads.keySet())) {
						return Promise.of(Optional.of(toHeads));
					}
				}
				return Promise.of(Optional.empty());
			}
		});
	}

	public static <K, D> Promise<List<D>> checkout(AsyncOTRepository<K, D> repository, OTSystem<D> system) {
		Ref<List<D>> cachedSnapshotRef = new Ref<>();
		return repository.getHeads()
				.then(heads ->
						findParent(repository, system, heads, DiffsReducer.toVoid(),
								commit -> repository.loadSnapshot(commit.getId())
										.map(maybeSnapshot -> (cachedSnapshotRef.value = maybeSnapshot.orElse(null)) != null))
								.then(findResult -> Promise.of(cachedSnapshotRef.value)))
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public static <K, D> Promise<List<D>> checkout(AsyncOTRepository<K, D> repository, OTSystem<D> system, K commitId) {
		Ref<List<D>> cachedSnapshotRef = new Ref<>();
		return repository.getHeads()
				.then(heads ->
						findParent(repository, system, union(heads, Set.of(commitId)), DiffsReducer.toVoid(),
								commit -> repository.loadSnapshot(commit.getId())
										.map(maybeSnapshot -> (cachedSnapshotRef.value = maybeSnapshot.orElse(null)) != null))
								.then(findResult -> diff(repository, system, findResult.commit, commitId)
										.map(diff -> concat(cachedSnapshotRef.value, diff))))
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	public static <K, D> Promise<Void> saveSnapshot(AsyncOTRepository<K, D> repository, OTSystem<D> system, K revisionId) {
		return checkout(repository, system, revisionId)
				.then(diffs -> repository.saveSnapshot(revisionId, diffs));
	}

	private static class LoadGraphReducer<K, D> implements GraphReducer<K, D, OTLoadedGraph<K, D>> {
		private final OTSystem<D> system;
		private final OTLoadedGraph<K, D> graph;
		private final Map<K, Set<K>> head2roots = new HashMap<>();
		private final Map<K, Set<K>> root2heads = new HashMap<>();

		private LoadGraphReducer(OTSystem<D> system) {
			this.system = system;
			this.graph = new OTLoadedGraph<>(system);
		}

		@Override
		public void onStart(Collection<OTCommit<K, D>> queue) {
			for (OTCommit<K, D> headCommit : queue) {
				K head = headCommit.getId();
				head2roots.put(head, new HashSet<>(List.of(head)));
				root2heads.put(head, new HashSet<>(List.of(head)));
			}
		}

		@Override
		public Promise<Result<OTLoadedGraph<K, D>>> onCommit(OTCommit<K, D> commit) {
			K node = commit.getId();
			Map<K, List<D>> parents = commit.getParents();

			Set<K> affectedHeads = root2heads.remove(node);
			for (K affectedHead : affectedHeads) {
				head2roots.get(affectedHead).remove(node);
			}
			for (K parent : commit.isRoot() ? Set.of(node) : parents.keySet()) {
				Set<K> parentRoots = graph.findRoots(parent);
				for (K affectedHead : affectedHeads) {
					head2roots.computeIfAbsent(affectedHead, $ -> new HashSet<>()).addAll(parentRoots);
				}
				for (K parentRoot : parentRoots) {
					root2heads.computeIfAbsent(parentRoot, $ -> new HashSet<>()).addAll(affectedHeads);
				}
			}

			graph.addNode(node, commit.getLevel(), parents);
			for (Map.Entry<K, List<? extends D>> entry : nonNullElseEmpty(graph.getChildren(node)).entrySet()) {
				K child = entry.getKey();
				List<? extends D> childDiffs = entry.getValue();
				Map<K, List<? extends D>> grandChildren = nonNullElseEmpty(graph.getChildren(child));
				Map<K, List<? extends D>> coParents = nonNullElseEmpty(graph.getParents(child));
				if (grandChildren.size() != 1 || coParents.size() != 1) continue;
				K grandChild = first(grandChildren.keySet());
				List<? extends D> grandChildDiffs = first(grandChildren.values());
				graph.addEdge(node, grandChild, system.squash(concat(childDiffs, grandChildDiffs)));
				graph.removeNode(child);
			}

			if (head2roots.keySet()
					.stream()
					.anyMatch(head -> head2roots.get(head).equals(root2heads.keySet()))) {
				return completePromise(graph);
			}
			return resumePromise();
		}
	}

	public static <K, D> Promise<OTLoadedGraph<K, D>> loadForMerge(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads) {
		return reduce(repository, system, heads, new LoadGraphReducer<>(system))
				.whenComplete(toLogger(logger, thisMethod(), heads));
	}

	public static <K, D> Promise<OTLoadedGraph<K, D>> loadGraph(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads, OTLoadedGraph<K, D> graph) {
		return reduce(repository, system, heads,
				commit -> {
					if (graph.hasChild(commit.getId())) {
						return skipPromise();
					}
					graph.addNode(commit.getId(), commit.getLevel(), commit.getParents());
					return resumePromise();
				})
				.then((v, e) -> {
					if (e instanceof GraphExhaustedException) return Promise.of(null);
					return Promise.of(v, e);
				})
				.map($ -> graph)
				.whenComplete(toLogger(logger, thisMethod(), heads, graph));
	}

	public static <K, D> Promise<OTLoadedGraph<K, D>> loadGraph(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads) {
		return loadGraph(repository, system, heads, new OTLoadedGraph<>(system));
	}

	public static <K, D> Promise<OTLoadedGraph<K, D>> loadGraph(AsyncOTRepository<K, D> repository, OTSystem<D> system, Set<K> heads, Function<K, String> idToString, Function<D, String> diffToString) {
		return loadGraph(repository, system, heads, new OTLoadedGraph<>(system, idToString, diffToString));
	}

	public static <K, D> Promise<Void> copy(AsyncOTRepository<K, D> from, AsyncOTRepository<K, D> to) {
		return from.getHeads()
				.then(heads -> toList(heads.stream().map(from::loadCommit))
						.then(commits -> {
							PriorityQueue<OTCommit<K, D>> queue = new PriorityQueue<>(reverseOrder(comparingLong(OTCommit::getLevel)));
							queue.addAll(commits);
							return Promises.repeat(
									() -> {
										if (queue.isEmpty()) return Promise.of(false);
										OTCommit<K, D> commit = queue.poll();
										return to.hasCommit(commit.getId())
												.thenIfElse(hasCommit -> hasCommit,
														$ -> Promise.complete(),
														$ -> Promises.all(commit.getParents().keySet().stream()
																		.map(parentId -> from.loadCommit(parentId)
																				.whenResult(parent -> !queue.contains(parent), queue::add)))
																.then(() -> to.push(commit)))
												.map($ -> true);
									});

						})
						.then(() -> to.updateHeads(heads, Set.of())));
	}

}
