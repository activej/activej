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

import io.activej.ot.exception.OTException;
import io.activej.ot.system.OTSystem;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.*;
import static java.util.Collections.*;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.*;

@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
public class OTLoadedGraph<K, D> {
	private final AtomicLong mergeId = new AtomicLong();

	public OTLoadedGraph(OTSystem<D> otSystem) {
		this.otSystem = otSystem;
	}

	public OTLoadedGraph(OTSystem<D> otSystem, @Nullable Function<K, String> idToString, @Nullable Function<D, String> diffToString) {
		this.otSystem = otSystem;
		this.idToString = nonNullElse(idToString, this.idToString);
		this.diffToString = nonNullElse(diffToString, this.diffToString);
	}

	private static final class MergeNode {
		final long n;

		private MergeNode(long n) {
			this.n = n;
		}

		@Override
		public String toString() {
			return "@" + n;
		}
	}

	private int compareNodes(K node1, K node2) {
		if (node1 instanceof MergeNode) {
			if (node2 instanceof MergeNode) {
				return Long.compare(((MergeNode) node1).n, ((MergeNode) node2).n);
			} else {
				return +1;
			}
		} else {
			if (node2 instanceof MergeNode) {
				return -1;
			} else {
				Long level1 = levels.getOrDefault(node1, 0L);
				Long level2 = levels.getOrDefault(node2, 0L);
				return Long.compare(level1, level2);
			}
		}
	}

	private final OTSystem<D> otSystem;

	private final Map<K, Map<K, List<? extends D>>> child2parent = new HashMap<>();
	private final Map<K, Map<K, List<? extends D>>> parent2child = new HashMap<>();
	private final Map<K, Long> levels = new HashMap<>();
	private Function<K, String> idToString = Objects::toString;
	private Function<D, String> diffToString = Objects::toString;

	public void addNode(K node, long level) {
		addNode(node, level, emptyMap());
	}

	public void addNode(K child, long level, Map<K, List<D>> parents) {
		levels.put(child, level);
		parents.forEach((parent, diffs) -> addEdge(parent, child, diffs));
	}

	public void addEdge(K parent, K child, List<? extends D> diff) {
		child2parent.computeIfAbsent(child, $ -> new HashMap<>()).put(parent, diff);
		parent2child.computeIfAbsent(parent, $ -> new HashMap<>()).put(child, diff);
	}

	public void removeNode(K node) {
		Set<K> parents = new HashSet<>(child2parent.getOrDefault(node, emptyMap()).keySet());
		Set<K> children = new HashSet<>(parent2child.getOrDefault(node, emptyMap()).keySet());
		parents.forEach(parent -> parent2child.get(parent).remove(node));
		children.forEach(child -> child2parent.get(child).remove(node));
		child2parent.remove(node);
		parent2child.remove(node);
		levels.remove(node);
	}

	public void removeEdge(K parent, K child) {
		parent2child.get(parent).remove(child);
		child2parent.get(child).remove(parent);
	}

	public void setLevel(K node, long level) {
		levels.put(node, level);
	}

	public boolean hasParent(K node) {
		return parent2child.containsKey(node);
	}

	public boolean hasChild(K node) {
		return child2parent.containsKey(node);
	}

	public Map<K, List<? extends D>> getParents(K child) {
		return child2parent.get(child);
	}

	public Map<K, List<? extends D>> getChildren(K parent) {
		return parent2child.get(parent);
	}

	public Set<K> getRoots() {
		return Stream.concat(levels.keySet().stream(), parent2child.keySet().stream())
				.filter(not(child2parent::containsKey))
				.collect(toSet());
	}

	public Set<K> getTips() {
		return Stream.concat(levels.keySet().stream(), child2parent.keySet().stream())
				.filter(not(parent2child::containsKey))
				.collect(toSet());
	}

	public List<D> findParent(K parent, K child) {
		if (child.equals(parent)) return emptyList();
		Set<K> visited = new HashSet<>();
		PriorityQueue<K> queue = new PriorityQueue<>(this::compareNodes);
		queue.add(child);
		Map<K, List<D>> result = new HashMap<>();
		result.put(child, emptyList());
		while (!queue.isEmpty()) {
			K node = queue.poll();
			List<D> node2child = result.remove(node);
			if (!visited.add(node)) continue;
			assert node2child != null;
			Map<K, List<? extends D>> nodeParents = nonNullElseEmpty(getParents(node));
			for (Map.Entry<K, List<? extends D>> entry : nodeParents.entrySet()) {
				K nodeParent = entry.getKey();
				if (!visited.contains(nodeParent) && !result.containsKey(nodeParent)) {
					List<D> parent2child = concat(entry.getValue(), node2child);
					if (nodeParent.equals(parent)) return parent2child;
					result.put(nodeParent, parent2child);
					queue.add(nodeParent);
				}
			}
		}
		throw new AssertionError();
	}

	public Set<K> findRoots(K node) {
		Set<K> result = new HashSet<>();
		Set<K> visited = new HashSet<>();
		ArrayList<K> queue = new ArrayList<>(singletonList(node));
		while (!queue.isEmpty()) {
			K node1 = queue.remove(queue.size() - 1);
			if (!visited.add(node1)) continue;
			Map<K, ? extends List<?>> parents = getParents(node1);
			if (parents == null) {
				result.add(node1);
			} else {
				queue.addAll(parents.keySet());
			}
		}
		return result;
	}

	public Set<K> excludeParents(Set<K> nodes) {
		Set<K> result = new LinkedHashSet<>(nodes);
		if (result.size() <= 1) return result;
		Set<K> visited = new HashSet<>();
		ArrayList<K> queue = new ArrayList<>(nodes);
		while (!queue.isEmpty()) {
			K node = queue.remove(queue.size() - 1);
			if (!visited.add(node)) continue;
			for (K parent : nonNullElseEmpty(getParents(node)).keySet()) {
				result.remove(parent);
				if (!visited.contains(parent)) {
					queue.add(parent);
				}
			}
		}
		return result;
	}

	public Map<K, List<D>> merge(Set<K> nodes) throws OTException {
		checkArgument(nodes.size() >= 2, "Cannot merge less than 2 commits");
		K mergeNode = doMerge(excludeParents(nodes));
		assert mergeNode != null;
		PriorityQueue<K> queue = new PriorityQueue<>(this::compareNodes);
		queue.add(mergeNode);
		Map<K, List<D>> paths = new HashMap<>();
		Map<K, List<D>> result = new HashMap<>();
		paths.put(mergeNode, emptyList());
		Set<K> visited = new HashSet<>();
		while (!queue.isEmpty()) {
			K node = queue.poll();
			List<D> path = paths.remove(node);
			if (!visited.add(node)) continue;
			if (nodes.contains(node)) {
				result.put(node, path);
				if (result.size() == nodes.size()) {
					break;
				}
			}
			Map<K, List<? extends D>> parentsMap = nonNullElseEmpty(getParents(node));
			for (Map.Entry<K, List<? extends D>> entry : parentsMap.entrySet()) {
				K parent = entry.getKey();
				if (visited.contains(parent) || paths.containsKey(parent)) continue;
				paths.put(parent, concat(entry.getValue(), path));
				queue.add(parent);
			}
		}
		assert result.size() == nodes.size();
		return result.entrySet().stream()
				.collect(toMap(Map.Entry::getKey, ops -> otSystem.squash(ops.getValue())));
	}

	@SuppressWarnings("unchecked")
	private K doMerge(Set<K> nodes) throws OTException {
		assert !nodes.isEmpty();
		if (nodes.size() == 1) return first(nodes);

		Optional<K> min = nodes.stream().min(comparingInt((K node) -> findRoots(node).size()));
		K pivotNode = min.get();

		Map<K, List<? extends D>> pivotNodeParents = getParents(pivotNode);
		Set<K> recursiveMergeNodes = union(pivotNodeParents.keySet(), difference(nodes, singleton(pivotNode)));
		K mergeNode = doMerge(excludeParents(recursiveMergeNodes));
		K parentNode = first(pivotNodeParents.keySet());
		List<? extends D> parentToPivotNode = pivotNodeParents.get(parentNode);
		List<D> parentToMergeNode = findParent(parentNode, mergeNode);

		if (pivotNodeParents.size() > 1) {
			K resultNode = (K) new MergeNode(mergeId.incrementAndGet());
			addEdge(mergeNode, resultNode, emptyList());
			addEdge(pivotNode, resultNode,
					otSystem.squash(concat(otSystem.invert(parentToPivotNode), parentToMergeNode)));
			return resultNode;
		}

		if (pivotNodeParents.size() == 1) {
			TransformResult<D> transformed = otSystem.transform(parentToPivotNode, parentToMergeNode);
			K resultNode = (K) new MergeNode(mergeId.incrementAndGet());
			addEdge(mergeNode, resultNode, transformed.right);
			addEdge(pivotNode, resultNode, transformed.left);
			return resultNode;
		}

		throw new OTException("Graph cannot be merged");
	}

	public String toGraphViz() {
		Set<K> tips = getTips();
		K currentCommit = tips.isEmpty() ? first(getRoots()) : first(getTips());
		return toGraphViz(currentCommit);
	}

	public String toGraphViz(@Nullable K currentCommit) {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n");
		for (Map.Entry<K, Map<K, List<? extends D>>> entry : child2parent.entrySet()) {
			K child = entry.getKey();
			Map<K, List<? extends D>> parent2diffs = entry.getValue();
			String color = (parent2diffs.size() == 1) ? "color=blue; " : "";
			for (Map.Entry<K, List<? extends D>> parentAndDiffs : parent2diffs.entrySet()) {
				sb.append("\t" +
						nodeToGraphViz(child) +
						" -> " + nodeToGraphViz(parentAndDiffs.getKey()) +
						" [ dir=\"back\"; " + color + "label=\"" +
						diffsToGraphViz(parentAndDiffs.getValue()) +
						"\"];\n");
			}
			addStyle(sb, child, currentCommit);
		}

		Set<K> roots = getRoots();
		for (K root : roots) {
			addStyle(sb, root, currentCommit);
		}

		sb.append("\t{ rank=same; " +
				getTips().stream().map(this::nodeToGraphViz).collect(joining(" ")) +
				" }\n");
		sb.append("\t{ rank=same; " +
				roots.stream().map(this::nodeToGraphViz).collect(joining(" ")) +
				" }\n");
		sb.append("}\n");

		return sb.toString();
	}

	private void addStyle(StringBuilder sb, K node, @Nullable K revision) {
		sb.append("\t" +
				nodeToGraphViz(node) +
				" [style=filled fillcolor=" +
				(node.equals(revision) ? "green" : "white") +
				"];\n");
	}

	private String nodeToGraphViz(K node) {
		return "\"" + idToString.apply(node) + "\"";
	}

	private String diffsToGraphViz(List<? extends D> diffs) {
		return diffs.isEmpty() ? "∅" : diffs.stream().map(diffToString).collect(joining(",\n"));
	}

	@Override
	public String toString() {
		return "{nodes=" + union(child2parent.keySet(), parent2child.keySet()) +
				", edges:" + parent2child.values().stream().mapToInt(Map::size).sum() + '}';
	}
}
