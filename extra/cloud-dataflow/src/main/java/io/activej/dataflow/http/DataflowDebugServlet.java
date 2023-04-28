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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.activej.csp.binary.codec.ByteBufsCodec;
import io.activej.csp.net.IMessaging;
import io.activej.csp.net.Messaging;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.exception.DataflowException;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.messaging.DataflowRequest;
import io.activej.dataflow.messaging.DataflowRequest.GetTasks;
import io.activej.dataflow.messaging.DataflowResponse;
import io.activej.dataflow.messaging.DataflowResponse.PartitionData;
import io.activej.dataflow.messaging.DataflowResponse.Result;
import io.activej.dataflow.messaging.DataflowResponse.TaskData;
import io.activej.dataflow.messaging.DataflowResponse.TaskDescription;
import io.activej.dataflow.stats.NodeStat;
import io.activej.dataflow.stats.StatReducer;
import io.activej.http.*;
import io.activej.http.loader.IStaticLoader;
import io.activej.inject.Key;
import io.activej.inject.ResourceLocator;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpResponse.Builder.ok200;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.types.Types.parameterizedType;
import static java.util.stream.Collectors.toList;

public final class DataflowDebugServlet extends AbstractReactive implements AsyncServlet {
	private final AsyncServlet servlet;
	private final ByteBufsCodec<DataflowResponse, DataflowRequest> codec;

	public DataflowDebugServlet(Reactor reactor, List<Partition> partitions, Executor executor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec, ResourceLocator env) {
		super(reactor);
		this.codec = codec;

		ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
		objectMapper.configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false);

		IStaticLoader staticLoader = IStaticLoader.ofClassPath(reactor, executor, "debug");

		this.servlet = RoutingServlet.create(reactor)
				.map("/*", StaticServlet.builder(reactor, staticLoader)
						.withIndexHtml()
						.build())
				.map("/api/*", RoutingServlet.create(reactor)
						.map(GET, "/partitions", request -> ok200()
								.withJson(objectMapper.writeValueAsString(partitions.stream()
										.map(Partition::address)
										.collect(Collectors.toList())))
								.build())
						.map(GET, "/tasks", request ->
								Promises.toList(partitions.stream().map(p -> getPartitionData(p.address())))
										.map(partitionStats -> {
											Map<Long, List<@Nullable TaskStatus>> tasks = new HashMap<>();
											for (int i = 0; i < partitionStats.size(); i++) {
												PartitionData partitionStat = partitionStats.get(i);
												for (TaskDescription taskDesc : partitionStat.lastTasks()) {
													tasks.computeIfAbsent(taskDesc.id(), $ -> Arrays.asList(new TaskStatus[partitionStats.size()]))
															.set(i, taskDesc.status());
												}
											}
											return ok200()
													.withJson(objectMapper.writeValueAsString(tasks))
													.build();
										}))
						.map(GET, "/tasks/:taskID", request -> {
							long id = getTaskId(request);
							return Promises.toList(partitions.stream().map(p -> getTask(p.address(), id)).collect(toList()))
									.map(localStats -> {
										List<@Nullable TaskStatus> statuses = Arrays.asList(new TaskStatus[localStats.size()]);

										Map<Integer, List<@Nullable NodeStat>> nodeStats = new HashMap<>();

										for (int i = 0; i < localStats.size(); i++) {
											TaskData localTaskData = localStats.get(i);
											if (localTaskData == null) {
												continue;
											}
											statuses.set(i, localTaskData.status());
											int finalI = i;
											localTaskData.nodes()
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

										ReducedTaskData taskData = new ReducedTaskData(statuses, localStats.get(0).graphViz(), reduced);
										return ok200()
												.withJson(objectMapper.writeValueAsString(taskData))
												.build();
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
							return getTask(partition.address(), id)
									.map(task -> ok200()
											.withJson(objectMapper.writeValueAsString(new LocalTaskData(task.status(), task.graphViz(),
													task.nodes().entrySet().stream()
															.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
													task.startTime(),
													task.finishTime(),
													task.error())))
											.build());
						}));
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
		return TcpSocket.connect(getCurrentReactor(), address)
				.then(socket -> {
					IMessaging<DataflowResponse, DataflowRequest> messaging = Messaging.create(socket, codec);
					return DataflowClient.performHandshake(messaging)
							.then(() -> messaging.send(new GetTasks(null)))
							.then(messaging::receive)
							.map(response -> {
								messaging.close();
								if (response instanceof PartitionData partitionData) {
									return partitionData;
								}
								if (response instanceof Result result) {
									throw new DataflowException("Error on remote server " + address + ": " + result.error());
								}
								throw new DataflowException("Bad response from server");
							});
				});
	}

	private Promise<TaskData> getTask(InetSocketAddress address, long taskId) {
		return TcpSocket.connect(getCurrentReactor(), address)
				.then(socket -> {
					IMessaging<DataflowResponse, DataflowRequest> messaging = Messaging.create(socket, codec);
					return DataflowClient.performHandshake(messaging)
							.then(() -> messaging.send(new GetTasks(taskId)))
							.then($ -> messaging.receive())
							.map(response -> {
								messaging.close();
								if (response instanceof TaskData taskData) {
									return taskData;
								}
								if (response instanceof Result result) {
									throw new DataflowException("Error on remote server " + address + ": " + result.error());
								}
								throw new DataflowException("Bad response from server");
							});
				});
	}

	@Override
	public Promisable<HttpResponse> serve(HttpRequest request) throws Exception {
		checkInReactorThread(this);
		return servlet.serve(request);
	}
}
