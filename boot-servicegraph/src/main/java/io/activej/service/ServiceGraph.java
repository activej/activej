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

package io.activej.service;

import io.activej.common.initializer.WithInitializer;
import io.activej.common.time.Stopwatch;
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.StringFormatUtils.formatDuration;
import static io.activej.common.Utils.concat;
import static io.activej.common.Utils.difference;
import static io.activej.inject.util.ReflectionUtils.getDisplayName;
import static io.activej.inject.util.Utils.getDisplayString;
import static io.activej.inject.util.Utils.union;
import static io.activej.service.Utils.combineAll;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.Comparator.comparingLong;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Stores the dependency graph of services. Primarily used by
 * {@link ServiceGraphModule}.
 */
@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
public final class ServiceGraph implements WithInitializer<ServiceGraph>, ConcurrentJmxBean {
	private static final Logger logger = LoggerFactory.getLogger(ServiceGraph.class);

	public interface Key {
		@NotNull Type getType();

		@Nullable Object getQualifier();

		@Nullable String getSuffix();

		@Nullable String getIndex();
	}

	private Runnable startCallback;

	private boolean started;

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes
	 * and between them exists edge from N1 to N2, it can be represent as
	 * adding to this SetMultimap element <N1,N2>. This collection consist of
	 * nodes in which there are edges and their keys - previous nodes.
	 */
	private final Map<Key, Set<Key>> forwards = new HashMap<>();

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes
	 * and between them exists edge from N1 to N2, it can be represent as
	 * adding to this SetMultimap element <N2,N1>. This collection consist of
	 * nodes in which there are edges and their keys - previous nodes.
	 */
	private final Map<Key, Set<Key>> backwards = new HashMap<>();

	private final Map<Key, Service> services = new HashMap<>();

	private volatile long startBegin;
	private volatile long startEnd;
	private volatile Throwable startException;
	private volatile SlowestChain slowestChain;

	private volatile long stopBegin;
	private volatile long stopEnd;
	private volatile Throwable stopException;

	private static final class NodeStatus {
		private static final NodeStatus DEFAULT = new NodeStatus();

		volatile long startBegin;
		volatile long startEnd;
		volatile Throwable startException;

		volatile long stopBegin;
		volatile long stopEnd;
		volatile Throwable stopException;

		private enum Operation {
			NEW, STARTING, STARTED, STOPPING, STOPPED, EXCEPTION
		}

		Operation getOperation() {
			if (startException != null || stopException != null) {
				return Operation.EXCEPTION;
			}
			if (stopEnd != 0) {
				return Operation.STOPPED;
			}
			if (stopBegin != 0) {
				return Operation.STOPPING;
			}
			if (startEnd != 0) {
				return Operation.STARTED;
			}
			if (startBegin != 0) {
				return Operation.STARTING;
			}
			return Operation.NEW;
		}

		boolean isStarting() {
			return startBegin != 0 && startEnd == 0;
		}

		boolean isStarted() {
			return startEnd != 0;
		}

		boolean isStartedSuccessfully() {
			return startEnd != 0 && startException == null;
		}

		boolean isStopping() {
			return stopBegin != 0 && stopEnd == 0;
		}

		boolean isStopped() {
			return stopEnd != 0;
		}

		long getStartTime() {
			checkState(startBegin != 0L && startEnd != 0L, "Start() has not been called or has not finished yet");
			return startEnd - startBegin;
		}

		long getStopTime() {
			checkState(stopBegin != 0L && stopEnd != 0L, "Stop() has not been called or has not finished yet");
			return stopEnd - stopBegin;
		}
	}

	private final Map<Key, NodeStatus> nodeStatuses = new ConcurrentHashMap<>();

	private String graphvizGraph = "rankdir=LR";
	private String graphvizStarting = "color=green";
	private String graphvizStarted = "color=blue";
	private String graphvizStopping = "color=green";
	private String graphvizStopped = "color=grey";
	private String graphvizException = "color=red";
	private String graphvizNodeWithSuffix = "peripheries=2";
	private String graphvizSlowestNode = "style=bold";
	private String graphvizSlowestEdge = "color=blue style=bold";
	private String graphvizEdge = "";

	private ServiceGraph() {
	}

	public static ServiceGraph create() {
		return new ServiceGraph();
	}

	void setStartCallback(Runnable startCallback) {
		this.startCallback = startCallback;
	}

	public ServiceGraph withGraphvizGraph(String graphvizGraph) {
		this.graphvizGraph = graphvizGraph;
		return this;
	}

	public ServiceGraph withGraphvizStarting(String graphvizStarting) {
		this.graphvizStarting = toGraphvizAttribute(graphvizStarting);
		return this;
	}

	public ServiceGraph withGraphvizStarted(String graphvizStarted) {
		this.graphvizStarted = toGraphvizAttribute(graphvizStarted);
		return this;
	}

	public ServiceGraph withGraphvizStopping(String graphvizStopping) {
		this.graphvizStopping = toGraphvizAttribute(graphvizStopping);
		return this;
	}

	public ServiceGraph withGraphvizStopped(String graphvizStopped) {
		this.graphvizStopped = toGraphvizAttribute(graphvizStopped);
		return this;
	}

	public ServiceGraph withGraphvizException(String graphvizException) {
		this.graphvizException = toGraphvizAttribute(graphvizException);
		return this;
	}

	public ServiceGraph withGraphvizEdge(String graphvizEdge) {
		this.graphvizEdge = toGraphvizAttribute(graphvizEdge);
		return this;
	}

	public ServiceGraph withGraphvizNodeWithSuffix(String graphvizNodeWithSuffix) {
		this.graphvizNodeWithSuffix = toGraphvizAttribute(graphvizNodeWithSuffix);
		return this;
	}

	public ServiceGraph withGraphvizSlowestNode(String graphvizSlowestNode) {
		this.graphvizSlowestNode = toGraphvizAttribute(graphvizSlowestNode);
		return this;
	}

	public ServiceGraph withGraphvizSlowestEdge(String graphvizSlowestEdge) {
		this.graphvizSlowestEdge = toGraphvizAttribute(graphvizSlowestEdge);
		return this;
	}

	private static String toGraphvizAttribute(String colorOrAttribute) {
		if (colorOrAttribute.isEmpty() || colorOrAttribute.contains("=")) {
			return colorOrAttribute;
		}
		return "color=" + (colorOrAttribute.startsWith("#") ? "\"" + colorOrAttribute + "\"" : colorOrAttribute);
	}

	public void add(Key key, @Nullable Service service, Key... dependencies) {
		checkArgument(!services.containsKey(key), "Key has already been added");
		if (service != null) {
			services.put(key, service);
		}
		add(key, asList(dependencies));
	}

	public void add(Key key, Collection<Key> dependencies) {
		for (Key dependency : dependencies) {
			forwards.computeIfAbsent(key, o -> new HashSet<>()).add(dependency);
			backwards.computeIfAbsent(dependency, o -> new HashSet<>()).add(key);
		}
	}

	public void add(Key key, Key first, Key... rest) {
		add(key, concat(singletonList(first), asList(rest)));
	}

	public synchronized boolean isStarted() {
		return started;
	}

	/**
	 * Start services in the service graph
	 */
	public synchronized CompletableFuture<?> startFuture() {
		if (started) {
			return CompletableFuture.completedFuture(false);
		}
		started = true;
		if (startCallback != null) {
			startCallback.run();
		}
		List<Key> circularDependencies = findCircularDependencies();
		checkState(circularDependencies == null, "Circular dependencies found: %s", circularDependencies);
		Set<Key> rootNodes = difference(union(services.keySet(), forwards.keySet()), backwards.keySet());
		if (rootNodes.isEmpty()) {
			throw new IllegalStateException("No root nodes found, nobody requested a service");
		}
		logger.info("Starting services");
		logger.trace("Root nodes: {}", rootNodes);
		startBegin = currentTimeMillis();
		return doStartStop(true, rootNodes)
				.whenComplete(($, e) -> {
					startEnd = currentTimeMillis();
					if (e == null) {
						slowestChain = findSlowestChain(rootNodes);
					} else {
						startException = e;
					}
				})
				.toCompletableFuture();
	}

	/**
	 * Stop services from the service graph
	 */
	public synchronized CompletableFuture<?> stopFuture() {
		Set<Key> leafNodes = difference(union(services.keySet(), backwards.keySet()), forwards.keySet());
		logger.info("Stopping services");
		logger.trace("Leaf nodes: {}", leafNodes);
		stopBegin = currentTimeMillis();
		return doStartStop(false, leafNodes)
				.whenComplete(($, e) -> {
					stopEnd = currentTimeMillis();
					if (e != null) {
						stopException = e;
					}
				})
				.toCompletableFuture();
	}

	private CompletionStage<?> doStartStop(boolean start, Collection<Key> rootNodes) {
		Map<Key, CompletionStage<?>> cache = new HashMap<>();
		return combineAll(
				rootNodes.stream()
						.map(rootNode -> processNode(rootNode, start, cache))
						.collect(toList()));
	}

	private synchronized CompletionStage<?> processNode(Key node, boolean start, Map<Key, CompletionStage<?>> cache) {

		if (cache.containsKey(node)) {
			CompletionStage<?> future = cache.get(node);
			if (logger.isTraceEnabled()) {
				logger.trace("{} : reusing {}", keyToString(node), future);
			}
			return future;
		}

		Set<Key> dependencies = (start ? forwards : backwards).getOrDefault(node, emptySet());

		if (logger.isTraceEnabled()) {
			logger.trace("{} : processing {}", keyToString(node), dependencies);
		}

		CompletableFuture<?> future = combineAll(
				dependencies.stream()
						.map(dependency -> processNode(dependency, start, cache))
						.collect(toList()))
				.thenCompose($ -> {
					Service service = services.get(node);
					if (service == null) {
						logger.trace("...skipping no-service node: {}", keyToString(node));
						return CompletableFuture.completedFuture(null);
					}

					if (!start && !nodeStatuses.getOrDefault(node, NodeStatus.DEFAULT).isStartedSuccessfully()) {
						logger.trace("...skipping not running node: {}", keyToString(node));
						return CompletableFuture.completedFuture(null);
					}

					Stopwatch sw = Stopwatch.createStarted();
					logger.trace("{} {} ...", start ? "Starting" : "Stopping", keyToString(node));
					NodeStatus nodeStatus = nodeStatuses.computeIfAbsent(node, $1 -> new NodeStatus());
					if (start) {
						nodeStatus.startBegin = currentTimeMillis();
					} else {
						nodeStatus.stopBegin = currentTimeMillis();
					}
					return (start ? service.start() : service.stop())
							.whenComplete(($2, e) -> {
								if (start) {
									nodeStatus.startEnd = currentTimeMillis();
									nodeStatus.startException = e;
								} else {
									nodeStatus.stopEnd = currentTimeMillis();
									nodeStatus.stopException = e;
								}

								long elapsed = sw.elapsed(MILLISECONDS);
								if (e == null) {
									logger.info((start ? "Started " : "Stopped ") + keyToString(node) + (elapsed >= 1L ? (" in " + sw) : ""));
								} else {
									logger.error((start ? "Start error " : "Stop error ") + keyToString(node),
											(e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null ? e.getCause() : e);
								}
							});
				});

		cache.put(node, future);

		return future;
	}

	private static void removeValue(Map<Key, Set<Key>> map, Key key, Key value) {
		Set<Key> objects = map.get(key);
		objects.remove(value);
		if (objects.isEmpty()) {
			map.remove(key);
		}
	}

	private void removeIntermediateOneWay(Key vertex, Map<Key, Set<Key>> forwards, Map<Key, Set<Key>> backwards) {
		for (Key backward : backwards.getOrDefault(vertex, emptySet())) {
			removeValue(forwards, backward, vertex);
			for (Key forward : forwards.getOrDefault(vertex, emptySet())) {
				if (!forward.equals(backward)) {
					forwards.computeIfAbsent(backward, o -> new HashSet<>()).add(forward);
				}
			}
		}
	}

	private void removeIntermediate(Key vertex) {
		removeIntermediateOneWay(vertex, forwards, backwards);
		removeIntermediateOneWay(vertex, backwards, forwards);
		forwards.remove(vertex);
		backwards.remove(vertex);
	}

	/**
	 * Removes nodes which don't have services
	 */
	public void removeIntermediateNodes() {
		List<Key> toRemove = new ArrayList<>();
		for (Key v : union(forwards.keySet(), backwards.keySet())) {
			if (!services.containsKey(v)) {
				toRemove.add(v);
			}
		}

		for (Key v : toRemove) {
			removeIntermediate(v);
		}
	}

	private @Nullable List<Key> findCircularDependencies() {
		Set<Key> visited = new LinkedHashSet<>();
		List<Key> path = new ArrayList<>();
		next:
		while (true) {
			for (Key node : path.isEmpty() ? services.keySet() : forwards.getOrDefault(path.get(path.size() - 1), emptySet())) {
				int loopIndex = path.indexOf(node);
				if (loopIndex != -1) {
					logger.warn("Circular dependencies found: {}", path.subList(loopIndex, path.size()).stream()
							.map(this::keyToString)
							.collect(joining(", ", "[", "]")));
					return path.subList(loopIndex, path.size());
				}
				if (!visited.contains(node)) {
					visited.add(node);
					path.add(node);
					continue next;
				}
			}
			if (path.isEmpty()) {
				break;
			}
			path.remove(path.size() - 1);
		}
		return null;
	}

	private static final class SlowestChain {
		static final SlowestChain EMPTY = new SlowestChain(emptyList(), 0);

		final List<Key> path;
		final long sum;

		private SlowestChain(List<Key> path, long sum) {
			this.path = path;
			this.sum = sum;
		}

		SlowestChain concat(Key key, long time) {
			return new SlowestChain(io.activej.common.Utils.concat(path, singletonList(key)), sum + time);
		}

		static SlowestChain of(Key key, long keyValue) {
			return new SlowestChain(singletonList(key), keyValue);
		}
	}

	private SlowestChain findSlowestChain(Collection<Key> nodes) {
		return nodes.stream()
				.map(node -> {
					Set<Key> children = forwards.get(node);
					if (children != null && !children.isEmpty()) {
						return findSlowestChain(children).concat(node, nodeStatuses.get(node).getStartTime());
					}
					return SlowestChain.of(node, nodeStatuses.get(node).getStartTime());
				})
				.max(comparingLong(longestPath -> longestPath.sum))
				.orElse(SlowestChain.EMPTY);
	}

	private String keyToString(Key key) {
		Object qualifier = key.getQualifier();
		String keySuffix = key.getSuffix();
		String keyIndex = key.getIndex();
		return (qualifier != null ? qualifier + " " : "") +
				key.getType().getTypeName() +
				(keySuffix == null ? "" :
						"[" + keySuffix + "]") +
				(keyIndex == null ? "" :
						keyIndex.isEmpty() ? "" : " #" + keyIndex);
	}

	private String keyToNode(Key key) {
		String str = keyToString(key)
				.replace("\n", "\\n")
				.replace("\"", "\\\"");
		return "\"" + str + "\"";
	}

	private String keyToLabel(Key key) {
		Object qualifier = key.getQualifier();
		String keySuffix = key.getSuffix();
		String keyIndex = key.getIndex();
		NodeStatus status = nodeStatuses.get(key);
		String label = (qualifier != null ? getDisplayString(qualifier) + "\\n" : "") +
				getDisplayName(key.getType()) +
				(keySuffix == null ? "" :
						"[" + keySuffix + "]") +
				(keyIndex == null ? "" :
						keyIndex.isEmpty() ? "" : "#" + keyIndex) +
				(status != null && status.isStarted() ?
						"\\n" +
								formatDuration(Duration.ofMillis(status.getStartTime())) +
								(status.isStopped() ?
										" / " + formatDuration(Duration.ofMillis(status.getStopTime())) :
										"") :
						"") +
				(status != null && status.startException != null ? "\\n" + status.startException : "") +
				(status != null && status.stopException != null ? "\\n" + status.stopException : "");
		return label.replace("\"", "\\\"");
	}

	@Override
	public String toString() {
		return toGraphViz();
	}

	@JmxOperation
	public String toGraphViz() {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n");
		if (!graphvizGraph.isEmpty()) {
			sb.append("\t" + graphvizGraph + "\n");
		}
		for (Map.Entry<Key, Set<Key>> entry : forwards.entrySet()) {
			Key node = entry.getKey();
			for (Key dependency : entry.getValue()) {
				sb.append("\t" + keyToNode(node) + " -> " + keyToNode(dependency))
						.append(slowestChain != null &&
								slowestChain.path.contains(node) && slowestChain.path.contains(dependency) &&
								slowestChain.path.indexOf(node) == slowestChain.path.indexOf(dependency) + 1 ?
								" [" + graphvizSlowestEdge + "]" :
								(!graphvizEdge.isEmpty() ? " [" + graphvizEdge + "]" : ""))
						.append("\n");
			}
		}

		Map<NodeStatus.Operation, String> nodeColors = new EnumMap<>(NodeStatus.Operation.class);
		nodeColors.put(NodeStatus.Operation.STARTING, graphvizStarting);
		nodeColors.put(NodeStatus.Operation.STARTED, graphvizStarted);
		nodeColors.put(NodeStatus.Operation.STOPPING, graphvizStopping);
		nodeColors.put(NodeStatus.Operation.STOPPED, graphvizStopped);
		nodeColors.put(NodeStatus.Operation.EXCEPTION, graphvizException);

		sb.append("\n");
		for (Key key : union(services.keySet(), union(backwards.keySet(), forwards.keySet()))) {
			NodeStatus status = nodeStatuses.get(key);
			String nodeColor = status != null ? nodeColors.getOrDefault(status.getOperation(), "") : "";
			sb.append("\t" + keyToNode(key) + " [ label=\"" + keyToLabel(key))
					.append("\"" + (!nodeColor.isEmpty() ? " " + nodeColor : ""))
					.append(key.getSuffix() != null ? " " + graphvizNodeWithSuffix : "")
					.append(slowestChain != null && slowestChain.path.contains(key) ? " " + graphvizSlowestNode : "")
					.append(" ]\n");
		}

		sb.append("\n\t{ rank=same; " +
						difference(union(services.keySet(), backwards.keySet()), forwards.keySet())
								.stream()
								.map(this::keyToNode)
								.collect(joining(" ")))
				.append(" }\n");

		sb.append("}\n");
		return sb.toString();
	}

	@JmxAttribute
	public String getStartingNodes() {
		return union(services.keySet(), union(backwards.keySet(), forwards.keySet())).stream()
				.filter(node -> {
					NodeStatus status = nodeStatuses.get(node);
					return status != null && status.isStarting();
				})
				.map(this::keyToString)
				.collect(joining(", "));
	}

	@JmxAttribute
	public String getStoppingNodes() {
		return union(services.keySet(), union(backwards.keySet(), forwards.keySet())).stream()
				.filter(node -> {
					NodeStatus status = nodeStatuses.get(node);
					return status != null && status.isStopping();
				})
				.map(this::keyToString)
				.collect(joining(", "));
	}

	@JmxAttribute
	public @Nullable String getSlowestNode() {
		return union(services.keySet(), union(backwards.keySet(), forwards.keySet())).stream()
				.filter(key -> {
					NodeStatus nodeStatus = nodeStatuses.get(key);
					return nodeStatus != null && nodeStatus.isStarted();
				})
				.max(comparingLong(node -> nodeStatuses.get(node).getStartTime()))
				.map(node -> keyToString(node) +
						" : " +
						formatDuration(Duration.ofMillis(nodeStatuses.get(node).getStartTime())))
				.orElse(null);
	}

	@JmxAttribute
	public @Nullable String getSlowestChain() {
		if (slowestChain == null) {
			return null;
		}
		return slowestChain.path.stream()
				.map(this::keyToString)
				.collect(joining(", ", "[", "]")) +
				" : " +
				formatDuration(Duration.ofMillis(slowestChain.sum));
	}

	@JmxAttribute
	public @Nullable Duration getStartDuration() {
		if (startBegin == 0) {
			return null;
		}
		return Duration.ofMillis((startEnd != 0 ? startEnd : currentTimeMillis()) - startBegin);
	}

	@JmxAttribute
	public Throwable getStartException() {
		return startException;
	}

	@JmxAttribute
	public @Nullable Duration getStopDuration() {
		if (stopBegin == 0) {
			return null;
		}
		return Duration.ofMillis((stopEnd != 0 ? stopEnd : currentTimeMillis()) - stopBegin);
	}

	@JmxAttribute
	public Throwable getStopException() {
		return stopException;
	}

}
