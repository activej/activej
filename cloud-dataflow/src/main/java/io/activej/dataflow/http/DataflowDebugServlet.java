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

package io.activej.dataflow.http;

import io.activej.codec.StructuredCodec;
import io.activej.codec.json.JsonUtils;
import io.activej.common.exception.UncheckedException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.dataflow.command.*;
import io.activej.dataflow.command.DataflowResponsePartitionData.TaskDesc;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.stats.NodeStat;
import io.activej.dataflow.stats.StatReducer;
import io.activej.http.*;
import io.activej.inject.Key;
import io.activej.inject.ResourceLocator;
import io.activej.inject.util.Types;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executor;

import static io.activej.codec.StructuredCodecs.*;
import static io.activej.dataflow.inject.CodecsModule.codec;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpResponse.ok200;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class DataflowDebugServlet implements AsyncServlet {
	private final AsyncServlet servlet;
	private final ByteBufsCodec<DataflowResponse, DataflowCommand> codec;

	public DataflowDebugServlet(List<Partition> partitions, Executor executor, ByteBufsCodec<DataflowResponse, DataflowCommand> codec, ResourceLocator env) {
		this.codec = codec;

		StructuredCodec<ReducedTaskData> reducedTaskDataCodec = env.getInstance(codec(ReducedTaskData.class));
		StructuredCodec<LocalTaskData> localTaskDataCodec = env.getInstance(codec(LocalTaskData.class));

		StructuredCodec<TaskStatus> taskStatusCodec = env.getInstance(codec(TaskStatus.class));
		StructuredCodec<Map<Long, List<@Nullable TaskStatus>>> taskListCodec = ofMap(LONG_CODEC, ofList(taskStatusCodec.nullable()));

		this.servlet = RoutingServlet.create()
				.map("/*", StaticServlet.ofClassPath(executor, "debug").withIndexHtml())
				.map("/api/*", RoutingServlet.create()
						.map(GET, "/partitions", request -> ok200()
								.withJson(partitions.stream()
										.map(p -> "\"" + p.getAddress().getAddress().getHostAddress() + ":" + p.getAddress().getPort() + "\"")
										.collect(joining(",", "[", "]"))))
						.map(GET, "/tasks", request ->
								Promises.toList(partitions.stream().map(p -> getPartitionData(p.getAddress())))
										.map(partitionStats -> {
											Map<Long, List<@Nullable TaskStatus>> tasks = new HashMap<>();
											for (int i = 0; i < partitionStats.size(); i++) {
												DataflowResponsePartitionData partitionStat = partitionStats.get(i);
												for (TaskDesc taskDesc : partitionStat.getLast()) {
													tasks.computeIfAbsent(taskDesc.getId(), $ -> Arrays.asList(new TaskStatus[partitionStats.size()]))
															.set(i, taskDesc.getStatus());
												}
											}
											return ok200()
													.withJson(JsonUtils.toJson(taskListCodec, tasks));
										}))
						.map(GET, "/tasks/:taskID", request ->
								getTaskId(request).then(id ->
										Promises.toList(partitions.stream().map(p -> getTask(p.getAddress(), id)).collect(toList()))
												.map(localStats -> {
													List<@Nullable TaskStatus> statuses = Arrays.asList(new TaskStatus[localStats.size()]);

													Map<Integer, List<@Nullable NodeStat>> nodeStats = new HashMap<>();

													for (int i = 0; i < localStats.size(); i++) {
														DataflowResponseTaskData localTaskData = localStats.get(i);
														if (localTaskData == null) {
															continue;
														}
														statuses.set(i, localTaskData.getStatus());
														int finalI = i;
														localTaskData.getNodes()
																.forEach((index, nodeStat) ->
																		nodeStats.computeIfAbsent(index, $ -> Arrays.asList(new NodeStat[localStats.size()]))
																				.set(finalI, nodeStat));
													}

													Map<Integer, @Nullable NodeStat> reduced = nodeStats.entrySet().stream()
															.collect(HashMap::new, (m, e) -> {
																NodeStat r = reduce(e.getValue(), env);
																if (r != null) {
																	m.put(e.getKey(), r);
																}
															}, HashMap::putAll);

													ReducedTaskData taskData = new ReducedTaskData(statuses, localStats.get(0).getGraphViz(), reduced);
													return ok200().withJson(JsonUtils.toJson(reducedTaskDataCodec, taskData));
												})))
						.map(GET, "/tasks/:taskID/:index", request ->
								getTaskId(request).then(id -> {
									String indexParam = request.getPathParameter("index");
									Partition partition;
									try {
										partition = partitions.get(Integer.parseInt(indexParam));
									} catch (NumberFormatException | IndexOutOfBoundsException e) {
										return Promise.ofException(HttpError.ofCode(400, "Bad index"));
									}
									return getTask(partition.getAddress(), id)
											.map(task -> ok200()
													.withJson(JsonUtils.toJson(localTaskDataCodec,
															new LocalTaskData(task.getStatus(), task.getGraphViz(), task.getNodes(), task.getStartTime(), task.getFinishTime(), task.getErrorString()))));
								})));
	}

	@Nullable
	private static NodeStat reduce(List<NodeStat> stats, ResourceLocator env) {
		Optional<NodeStat> firstNonNull = stats.stream().filter(Objects::nonNull).findAny();
		if (!firstNonNull.isPresent()) {
			return null; // reduce all-null or empty lists to null
		}
		StatReducer<NodeStat> reducer = env.getInstanceOrNull(Key.ofType(Types.parameterized(StatReducer.class, firstNonNull.get().getClass())));
		if (reducer == null) {
			return null; // if no reducer is provided then return null, idk some stat type might not be reducible
		}
		return reducer.reduce(stats);
	}

	private static Promise<Long> getTaskId(HttpRequest request) {
		String param = request.getPathParameter("taskID");
		try {
			return Promise.of(Long.parseLong(param));
		} catch (NumberFormatException e) {
			return Promise.ofException(HttpError.ofCode(400, "Bad number " + param));
		}
	}

	private Promise<DataflowResponsePartitionData> getPartitionData(InetSocketAddress address) {
		return AsyncTcpSocketNio.connect(address)
				.then(socket -> {
					Messaging<DataflowResponse, DataflowCommand> messaging = MessagingWithBinaryStreaming.create(socket, codec);
					return messaging.send(new DataflowCommandGetTasks(null))
							.then($ -> messaging.receive())
							.then(response -> {
								messaging.close();
								if (response instanceof DataflowResponsePartitionData) {
									return Promise.of(((DataflowResponsePartitionData) response));
								} else if (response instanceof DataflowResponseResult) {
									return Promise.ofException(new Exception("Error on remote server " + address + ": " + ((DataflowResponseResult) response).getError()));
								}
								return Promise.ofException(new Exception("Bad response from server"));
							});
				});
	}

	private Promise<DataflowResponseTaskData> getTask(InetSocketAddress address, long taskId) {
		return AsyncTcpSocketNio.connect(address)
				.then(socket -> {
					Messaging<DataflowResponse, DataflowCommand> messaging = MessagingWithBinaryStreaming.create(socket, codec);
					return messaging.send(new DataflowCommandGetTasks(taskId))
							.then($ -> messaging.receive())
							.then(response -> {
								messaging.close();
								if (response instanceof DataflowResponseTaskData) {
									return Promise.of(((DataflowResponseTaskData) response));
								} else if (response instanceof DataflowResponseResult) {
									return Promise.ofException(new Exception("Error on remote server " + address + ": " + ((DataflowResponseResult) response).getError()));
								}
								return Promise.ofException(new Exception("Bad response from server"));
							});
				});
	}

	@Override
	@NotNull
	public Promisable<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		return servlet.serve(request);
	}
}
