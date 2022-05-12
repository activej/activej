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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowException;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest.GetTasks.TaskId;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse.PartitionData;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse.PartitionData.TaskDesc;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse.TaskData;
import io.activej.dataflow.stats.NodeStat;
import io.activej.dataflow.stats.StatReducer;
import io.activej.http.*;
import io.activej.inject.Key;
import io.activej.inject.ResourceLocator;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static io.activej.dataflow.proto.ProtobufUtils.convert;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpResponse.ok200;
import static io.activej.types.Types.parameterizedType;
import static java.util.stream.Collectors.toList;

public final class DataflowDebugServlet implements AsyncServlet {
	private final AsyncServlet servlet;
	private final ByteBufsCodec<DataflowResponse, DataflowRequest> codec;

	public DataflowDebugServlet(List<Partition> partitions, Executor executor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec, ResourceLocator env) {
		this.codec = codec;

		ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
		objectMapper.configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false);

		this.servlet = RoutingServlet.create()
				.map("/*", StaticServlet.ofClassPath(executor, "debug").withIndexHtml())
				.map("/api/*", RoutingServlet.create()
						.map(GET, "/partitions", request -> ok200()
								.withJson(objectMapper.writeValueAsString(partitions.stream()
										.map(Partition::getAddress)
										.collect(Collectors.toList()))))
						.map(GET, "/tasks", request ->
								Promises.toList(partitions.stream().map(p -> getPartitionData(p.getAddress())))
										.map(partitionStats -> {
											Map<Long, List<@Nullable TaskStatus>> tasks = new HashMap<>();
											for (int i = 0; i < partitionStats.size(); i++) {
												PartitionData partitionStat = partitionStats.get(i);
												for (TaskDesc taskDesc : partitionStat.getLastList()) {
													tasks.computeIfAbsent(taskDesc.getId(), $ -> Arrays.asList(new TaskStatus[partitionStats.size()]))
															.set(i, convert(taskDesc.getStatus()));
												}
											}
											return ok200()
													.withJson(objectMapper.writeValueAsString(tasks));
										}))
						.map(GET, "/tasks/:taskID", request -> {
							long id = getTaskId(request);
							return Promises.toList(partitions.stream().map(p -> getTask(p.getAddress(), id)).collect(toList()))
									.map(localStats -> {
										List<@Nullable TaskStatus> statuses = Arrays.asList(new TaskStatus[localStats.size()]);

										Map<Integer, List<@Nullable NodeStat>> nodeStats = new HashMap<>();

										for (int i = 0; i < localStats.size(); i++) {
											TaskData localTaskData = localStats.get(i);
											if (localTaskData == null) {
												continue;
											}
											statuses.set(i, convert(localTaskData.getStatus()));
											int finalI = i;
											localTaskData.getNodesMap()
													.forEach((index, nodeStat) ->
															nodeStats.computeIfAbsent(index, $ -> Arrays.asList(new NodeStat[localStats.size()]))
																	.set(finalI, convert(nodeStat)));
										}

										Map<Integer, @Nullable NodeStat> reduced = nodeStats.entrySet().stream()
												.collect(HashMap::new, (m, e) -> {
													NodeStat r = reduce(e.getValue(), env);
													if (r != null) {
														m.put(e.getKey(), r);
													}
												}, HashMap::putAll);

										ReducedTaskData taskData = new ReducedTaskData(statuses, localStats.get(0).getGraphViz(), reduced);
										return ok200()
												.withJson(objectMapper.writeValueAsString(taskData));
									});
						})
						.map(GET, "/tasks/:taskID/:index", request -> {
							long id = getTaskId(request);
							String indexParam = request.getPathParameter("index");
							Partition partition;
							try {
								partition = partitions.get(Integer.parseInt(indexParam));
							} catch (NumberFormatException | IndexOutOfBoundsException e) {
								throw HttpError.ofCode(400, "Bad index");
							}
							return getTask(partition.getAddress(), id)
									.map(task -> ok200()
											.withJson(objectMapper.writeValueAsString(new LocalTaskData(convert(task.getStatus()), task.getGraphViz(),
													task.getNodesMap().entrySet().stream()
															.collect(Collectors.toMap(Map.Entry::getKey, e -> convert(e.getValue()))),
													convert(task.getStartTime()),
													convert(task.getFinishTime()),
													convert(task.getError())))));
						}));
	}

	public static void main(String[] args) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		System.out.println(mapper.writeValueAsString(new Partition(new InetSocketAddress("sss", 123))));
	}

	private static @Nullable NodeStat reduce(List<NodeStat> stats, ResourceLocator env) {
		Optional<NodeStat> firstNonNull = stats.stream().filter(Objects::nonNull).findAny();
		if (firstNonNull.isEmpty()) {
			return null; // reduce all-null or empty lists to null
		}
		StatReducer<NodeStat> reducer = env.getInstanceOrNull(Key.ofType(parameterizedType(StatReducer.class, firstNonNull.get().getClass())));
		if (reducer == null) {
			return null; // if no reducer is provided then return null, IDK some stat type might not be reducible
		}
		return reducer.reduce(stats);
	}

	private static long getTaskId(HttpRequest request) throws HttpError {
		String param = request.getPathParameter("taskID");
		try {
			return Long.parseLong(param);
		} catch (NumberFormatException e) {
			throw HttpError.ofCode(400, "Bad number " + param);
		}
	}

	private Promise<PartitionData> getPartitionData(InetSocketAddress address) {
		return AsyncTcpSocketNio.connect(address)
				.then(socket -> {
					Messaging<DataflowResponse, DataflowRequest> messaging = MessagingWithBinaryStreaming.create(socket, codec);
					return DataflowClient.performHandshake(messaging)
							.then(() -> messaging.send(getTasks(null)))
							.then(messaging::receive)
							.map(response -> {
								messaging.close();
								switch (response.getResponseCase()) {
									case PARTITION_DATA:
										return response.getPartitionData();
									case RESULT:
										throw new DataflowException("Error on remote server " + address + ": " + response.getResult().getError().getError());
									default:
										throw new DataflowException("Bad response from server");
								}
							});
				});
	}

	private Promise<TaskData> getTask(InetSocketAddress address, long taskId) {
		return AsyncTcpSocketNio.connect(address)
				.then(socket -> {
					Messaging<DataflowResponse, DataflowRequest> messaging = MessagingWithBinaryStreaming.create(socket, codec);
					return DataflowClient.performHandshake(messaging)
							.then(() -> messaging.send(getTasks(taskId)))
							.then($ -> messaging.receive())
							.map(response -> {
								messaging.close();
								switch (response.getResponseCase()) {
									case TASK_DATA:
										return response.getTaskData();
									case RESULT:
										throw new DataflowException("Error on remote server " + address + ": " + response.getResult().getError().getError());
									default:
										throw new DataflowException("Bad response from server");
								}
							});
				});
	}

	@Override
	public @NotNull Promisable<HttpResponse> serve(@NotNull HttpRequest request) throws Exception {
		return servlet.serve(request);
	}

	private static DataflowRequest getTasks(@Nullable Long taskId) {
		TaskId.Builder taskIdBuilder = TaskId.newBuilder();
		if (taskId == null) {
			taskIdBuilder.setTaskIdIsNull(true);
		} else {
			taskIdBuilder.setTaskId(taskId);
		}
		return DataflowRequest.newBuilder()
				.setGetTasks(DataflowRequest.GetTasks.newBuilder()
						.setTaskId(taskIdBuilder))
				.build();
	}

}
