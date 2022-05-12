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

package io.activej.dataflow.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.DataflowException;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.node.*;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.NodeProto.Node.*;
import io.activej.dataflow.proto.NodeProto.Node.Download.Address;
import io.activej.dataflow.proto.NodeProto.Node.Reduce.Input;
import io.activej.dataflow.proto.NodeStatProto.NodeStat.Binary;
import io.activej.dataflow.proto.NodeStatProto.NodeStat.Test;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.NodeStat;
import io.activej.dataflow.stats.TestNodeStat;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.inject.util.Constructors;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public final class ProtobufUtils {

	public static <I extends Message, O extends Message> ByteBufsCodec<I, O> codec(Parser<I> inputParser) {
		return new ByteBufsCodec<>() {
			@Override
			public ByteBuf encode(O item) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try {
					item.writeDelimitedTo(baos);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
				return ByteBuf.wrapForReading(baos.toByteArray());
			}

			@Override
			public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
				try {
					PeekingInputStream peekingInputStream = new PeekingInputStream(bufs);
					I result = inputParser.parseDelimitedFrom(peekingInputStream);
					bufs.skip(peekingInputStream.offset);
					return result;
				} catch (InvalidProtocolBufferException e) {
					IOException ioException = e.unwrapIOException();
					if (ioException == NEED_MORE_DATA_EXCEPTION) {
						return null;
					}
					throw new MalformedDataException(e);
				}
			}
		};
	}

	private static final NeedMoreDataException NEED_MORE_DATA_EXCEPTION = new NeedMoreDataException();

	private static final class NeedMoreDataException extends IOException {
		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}
	}

	public static StreamIdProto.StreamId convert(StreamId streamId) {
		return StreamIdProto.StreamId.newBuilder().setId(streamId.getId()).build();
	}

	public static StreamId convert(StreamIdProto.StreamId streamId) {
		return new StreamId(streamId.getId());
	}

	public static List<NodeProto.Node> convert(Collection<Node> nodes, FunctionSerializer functionSerializer) {
		return nodes.stream()
				.map(node -> convert(node, functionSerializer))
				.collect(toList());
	}

	public static NodeProto.Node convert(Node node, FunctionSerializer functionSerializer) {
		NodeProto.Node.Builder builder = NodeProto.Node.newBuilder();
		if (node instanceof NodeReduce<?, ?, ?> reduce) {
			builder.setReduce(Reduce.newBuilder()
					.setIndex(reduce.getIndex())
					.setKeyComparator(functionSerializer.serializeComparator(reduce.getKeyComparator()))
					.putAllInputs(reduce.getInputMap().entrySet().stream()
							.collect(Collectors.toMap(e -> e.getKey().getId(), e -> Input.newBuilder()
									.setReducer(functionSerializer.serializeReducer(e.getValue().getReducer()))
									.setKeyFunction(functionSerializer.serializeFunction(e.getValue().getKeyFunction()))
									.build())))
					.setOutput(convert(reduce.getOutput()))
					.build());
		} else if (node instanceof NodeUpload<?> upload) {
			builder.setUpload(NodeProto.Node.Upload.newBuilder()
					.setIndex(upload.getIndex())
					.setType(upload.getType().getName())
					.setStreamId(convert(upload.getStreamId()))
					.build());
		} else if (node instanceof NodeSort<?, ?> sort) {
			builder.setSort(NodeProto.Node.Sort.newBuilder()
					.setIndex(sort.getIndex())
					.setType(sort.getType().getName())
					.setKeyFunction(functionSerializer.serializeFunction(sort.getKeyFunction()))
					.setKeyComparator(functionSerializer.serializeComparator(sort.getKeyComparator()))
					.setDeduplicate(sort.isDeduplicate())
					.setItemsInMemorySize(sort.getItemsInMemorySize())
					.setInput(convert(sort.getInput()))
					.setOutput(convert(sort.getOutput()))
					.build());
		} else if (node instanceof NodeMerge<?, ?> merge) {
			builder.setMerge(NodeProto.Node.Merge.newBuilder()
					.setIndex(merge.getIndex())
					.setKeyFunction(functionSerializer.serializeFunction(merge.getKeyFunction()))
					.setKeyComparator(functionSerializer.serializeComparator(merge.getKeyComparator()))
					.setDeduplicate(merge.isDeduplicate())
					.addAllInputs(convertIds(merge.getInputs()))
					.setOutput(convert(merge.getOutput()))
					.build());
		} else if (node instanceof NodeShard<?, ?> shard) {
			builder.setShard(NodeProto.Node.Shard.newBuilder()
					.setIndex(shard.getIndex())
					.setKeyFunction(functionSerializer.serializeFunction(shard.getKeyFunction()))
					.setInput(convert(shard.getInput()))
					.addAllOutputs(convertIds(shard.getOutputs()))
					.setNonce(shard.getNonce())
					.build());
		} else if (node instanceof NodeDownload<?> download) {
			builder.setDownload(Download.newBuilder()
					.setIndex(download.getIndex())
					.setType(download.getType().getName())
					.setAddress(Address.newBuilder()
							.setHost(download.getAddress().getHostName())
							.setPort(download.getAddress().getPort()))
					.setInput(convert(download.getStreamId()))
					.setOutput(convert(download.getOutput()))
					.build());
		} else if (node instanceof NodeMap<?, ?> map) {
			builder.setMap(NodeProto.Node.Map.newBuilder()
					.setIndex(map.getIndex())
					.setFunction(functionSerializer.serializeFunction(map.getFunction()))
					.setInput(convert(map.getInput()))
					.setOutput(convert(map.getOutput()))
					.build());
		} else if (node instanceof NodeJoin<?, ?, ?, ?> join) {
			builder.setJoin(Join.newBuilder()
					.setIndex(join.getIndex())
					.setLeft(convert(join.getLeft()))
					.setRight(convert(join.getRight()))
					.setOutput(convert(join.getOutput()))
					.setComparator(functionSerializer.serializeComparator(join.getKeyComparator()))
					.setLeftKeyFunction(functionSerializer.serializeFunction(join.getLeftKeyFunction()))
					.setRightKeyFunction(functionSerializer.serializeFunction(join.getRightKeyFunction()))
					.setJoiner(functionSerializer.serializeJoiner(join.getJoiner()))
					.build());
		} else if (node instanceof NodeFilter<?> filter) {
			builder.setFilter(Filter.newBuilder()
					.setIndex(filter.getIndex())
					.setPredicate(functionSerializer.serializePredicate(filter.getPredicate()))
					.setInput(convert(filter.getInput()))
					.setOutput(convert(filter.getOutput()))
					.build());
		} else if (node instanceof NodeConsumerOfId<?> consumerOfId) {
			builder.setConsumerOfId(ConsumerOfId.newBuilder()
					.setIndex(consumerOfId.getIndex())
					.setId(consumerOfId.getId())
					.setPartitionIndex(consumerOfId.getPartitionIndex())
					.setMaxPartitions(consumerOfId.getMaxPartitions())
					.setInput(convert(consumerOfId.getInput()))
					.build());
		} else if (node instanceof NodeSupplierOfId<?> supplierOfId) {
			builder.setSupplierOfId(NodeProto.Node.SupplierOfId.newBuilder()
					.setIndex(supplierOfId.getIndex())
					.setId(supplierOfId.getId())
					.setPartitionIndex(supplierOfId.getPartitionIndex())
					.setMaxPartitions(supplierOfId.getMaxPartitions())
					.setOutput(convert(supplierOfId.getOutput()))
					.build());
		} else if (node instanceof NodeReduceSimple<?, ?, ?, ?> reduceSimple) {
			builder.setReduceSimple(NodeProto.Node.ReduceSimple.newBuilder()
					.setIndex(reduceSimple.getIndex())
					.setKeyFunction(functionSerializer.serializeFunction(reduceSimple.getKeyFunction()))
					.setKeyComparator(functionSerializer.serializeComparator(reduceSimple.getKeyComparator()))
					.setReducer(functionSerializer.serializeReducer(reduceSimple.getReducer()))
					.addAllInputs(convertIds(reduceSimple.getInputs()))
					.setOutput(convert(reduceSimple.getOutput()))
					.build());
		} else if (node instanceof NodeUnion<?> union) {
			builder.setUnion(NodeProto.Node.Union.newBuilder()
					.setIndex(union.getIndex())
					.addAllInputs(convertIds(union.getInputs()))
					.setOutput(convert(union.getOutput()))
					.build());
		} else {
			throw new AssertionError();
		}
		return builder.build();
	}

	private static List<StreamIdProto.StreamId> convertIds(List<StreamId> ids) {
		return ids.stream()
				.map(ProtobufUtils::convert)
				.collect(toList());
	}

	private static List<StreamId> convertProtoIds(List<StreamIdProto.StreamId> ids) {
		return ids.stream()
				.map(ProtobufUtils::convert)
				.collect(toList());
	}

	public static List<Node> convert(List<NodeProto.Node> nodes, FunctionSerializer functionSerializer) throws DataflowException {
		List<Node> list = new ArrayList<>();
		for (NodeProto.Node node : nodes) {
			list.add(convert(node, functionSerializer));
		}
		return list;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public static Node convert(NodeProto.Node node, FunctionSerializer functionSerializer) throws DataflowException {
		try {
			switch (node.getNodeCase()) {
				case CONSUMER_OF_ID -> {
					ConsumerOfId coi = node.getConsumerOfId();
					return new NodeConsumerOfId<>(coi.getIndex(), coi.getId(), coi.getPartitionIndex(), coi.getMaxPartitions(), convert(coi.getInput()));
				}
				case DOWNLOAD -> {
					Download download = node.getDownload();
					Address address = download.getAddress();
					return new NodeDownload<>(download.getIndex(), getType(download.getType()), new InetSocketAddress(address.getHost(), address.getPort()), convert(download.getInput()), convert(download.getOutput()));
				}
				case FILTER -> {
					Filter filter = node.getFilter();
					return new NodeFilter<>(filter.getIndex(), functionSerializer.deserializePredicate(filter.getPredicate()), convert(filter.getInput()), convert(filter.getOutput()));
				}
				case JOIN -> {
					Join join = node.getJoin();
					Comparator joinComparator = functionSerializer.deserializeComparator(join.getComparator());
					Function leftKeyFunction = functionSerializer.deserializeFunction(join.getLeftKeyFunction());
					Function rightKeyFunction = functionSerializer.deserializeFunction(join.getRightKeyFunction());
					Joiner joiner = functionSerializer.deserializeJoiner(join.getJoiner());
					return new NodeJoin<>(join.getIndex(), convert(join.getLeft()), convert(join.getRight()), convert(join.getOutput()), joinComparator, leftKeyFunction, rightKeyFunction, joiner);
				}
				case MAP -> {
					NodeProto.Node.Map map = node.getMap();
					return new NodeMap<>(map.getIndex(), functionSerializer.deserializeFunction(map.getFunction()), convert(map.getInput()), convert(map.getOutput()));
				}
				case MERGE -> {
					Merge merge = node.getMerge();
					Function function = functionSerializer.deserializeFunction(merge.getKeyFunction());
					Comparator mergeComparator = functionSerializer.deserializeComparator(merge.getKeyComparator());
					return new NodeMerge<>(merge.getIndex(), function, mergeComparator, merge.getDeduplicate(), convertProtoIds(merge.getInputsList()), convert(merge.getOutput()));
				}
				case REDUCE -> {
					Reduce reduce = node.getReduce();
					Comparator reduceComparator = functionSerializer.deserializeComparator(reduce.getKeyComparator());
					return new NodeReduce<>(reduce.getIndex(), reduceComparator, convertInputs(functionSerializer, reduce.getInputsMap()), convert(reduce.getOutput()));
				}
				case REDUCE_SIMPLE -> {
					ReduceSimple reduceSimple = node.getReduceSimple();
					Function keyFunction = functionSerializer.deserializeFunction(reduceSimple.getKeyFunction());
					Comparator keyComparator = functionSerializer.deserializeComparator(reduceSimple.getKeyComparator());
					Reducer reducer = functionSerializer.deserializeReducer(reduceSimple.getReducer());
					return new NodeReduceSimple<>(reduceSimple.getIndex(), keyFunction, keyComparator, reducer, convertProtoIds(reduceSimple.getInputsList()), convert(reduceSimple.getOutput()));
				}
				case SHARD -> {
					Shard shard = node.getShard();
					return new NodeShard<>(shard.getIndex(), functionSerializer.deserializeFunction(shard.getKeyFunction()), convert(shard.getInput()), convertProtoIds(shard.getOutputsList()), shard.getNonce());
				}
				case SORT -> {
					Sort sort = node.getSort();
					Function sortFunction = functionSerializer.deserializeFunction(sort.getKeyFunction());
					Comparator sortComparator = functionSerializer.deserializeComparator(sort.getKeyComparator());
					return new NodeSort<>(sort.getIndex(), getType(sort.getType()), sortFunction, sortComparator, sort.getDeduplicate(), sort.getItemsInMemorySize(), convert(sort.getInput()), convert(sort.getOutput()));
				}
				case SUPPLIER_OF_ID -> {
					SupplierOfId sid = node.getSupplierOfId();
					return new NodeSupplierOfId<>(sid.getIndex(), sid.getId(), sid.getPartitionIndex(), sid.getMaxPartitions(), convert(sid.getOutput()));
				}
				case UNION -> {
					Union union = node.getUnion();
					return new NodeUnion<>(union.getIndex(), convertProtoIds(union.getInputsList()), convert(union.getOutput()));
				}
				case UPLOAD -> {
					Upload upload = node.getUpload();
					return new NodeUpload<>(upload.getIndex(), getType(upload.getType()), convert(upload.getStreamId()));
				}
				default -> throw new DataflowException("Node was not set");
			}
		} catch (MalformedDataException e) {
			throw new DataflowException(e);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Map<StreamId, NodeReduce.Input<Object, Object, Object>> convertInputs(FunctionSerializer functionSerializer, Map<Long, Input> inputsMap) throws MalformedDataException {
		Map<StreamId, NodeReduce.Input<Object, Object, Object>> result = new LinkedHashMap<>();
		for (Map.Entry<Long, Input> entry : inputsMap.entrySet()) {
			Function<?, ?> keyFunction = functionSerializer.deserializeFunction(entry.getValue().getKeyFunction());
			Reducer<?, ?, ?, ?> reducer = functionSerializer.deserializeReducer(entry.getValue().getReducer());

			result.put(new StreamId(entry.getKey()), new NodeReduce.Input(reducer, keyFunction));
		}
		return result;
	}

	public static DataflowResponse.TaskStatus convert(TaskStatus status) {
		return switch (status) {
			case RUNNING -> DataflowResponse.TaskStatus.RUNNING;
			case COMPLETED -> DataflowResponse.TaskStatus.COMPLETED;
			case FAILED -> DataflowResponse.TaskStatus.FAILED;
			case CANCELED -> DataflowResponse.TaskStatus.CANCELLED;
		};
	}

	public static TaskStatus convert(DataflowResponse.TaskStatus status) {
		return switch (status) {
			case RUNNING -> TaskStatus.RUNNING;
			case COMPLETED -> TaskStatus.COMPLETED;
			case FAILED -> TaskStatus.FAILED;
			case CANCELLED -> TaskStatus.CANCELED;
			default -> throw new AssertionError();
		};
	}

	public static DataflowResponse.Instant convert(@Nullable Instant instant) {
		DataflowResponse.Instant.Builder builder = DataflowResponse.Instant.newBuilder();
		if (instant == null) {
			builder.setInstantIsNull(true);
		} else {
			builder.setTimestamp(instant.toEpochMilli());
		}
		return builder.build();
	}

	public static @Nullable Instant convert(DataflowResponse.Instant instant) {
		return instant.getInstantIsNull() ? null : Instant.ofEpochMilli(instant.getTimestamp());
	}

	public static DataflowResponse.Error error(@Nullable String error) {
		DataflowResponse.Error.Builder builder = DataflowResponse.Error.newBuilder();
		if (error == null) {
			builder.setErrorIsNull(true);
		} else {
			builder.setError(error);
		}
		return builder.build();
	}

	public static @Nullable String convert(DataflowResponse.Error error) {
		return error.getErrorIsNull() ? null : error.getError();
	}

	public static NodeStatProto.NodeStat convert(NodeStat nodeStat) {
		if (nodeStat instanceof BinaryNodeStat) {
			return NodeStatProto.NodeStat.newBuilder()
					.setBinary(Binary.newBuilder().setBytes(((BinaryNodeStat) nodeStat).getBytes()))
					.build();
		}
		if (nodeStat instanceof TestNodeStat) {
			return NodeStatProto.NodeStat.newBuilder()
					.setTest(Test.newBuilder().setNodeIndex(((TestNodeStat) nodeStat).getNodeIndex()))
					.build();
		}
		throw new AssertionError();
	}

	public static NodeStat convert(NodeStatProto.NodeStat nodeStat) {
		return switch (nodeStat.getNodeStatCase()) {
			case BINARY -> new BinaryNodeStat(nodeStat.getBinary().getBytes());
			case TEST -> new TestNodeStat(nodeStat.getTest().getNodeIndex());
			default -> throw new AssertionError();
		};
	}

	public static <T> BinarySerializer<T> ofObject(Constructors.Constructor0<T> constructor) {
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, T item) {
			}

			@Override
			public T decode(BinaryInput in) throws CorruptedDataException {
				return constructor.create();
			}
		};
	}

	private static final Map<String, Class<?>> TYPE_CACHE = new ConcurrentHashMap<>();

	private static Class<?> getType(String typeString) throws DataflowException {
		Class<?> aType = TYPE_CACHE.computeIfAbsent(typeString, $ -> {
			try {
				return Class.forName(typeString);
			} catch (ClassNotFoundException e) {
				return null;
			}
		});
		if (aType == null) {
			throw new DataflowException("Cannot construct class: " + typeString);
		}

		return aType;
	}

	private static class PeekingInputStream extends InputStream {
		private final ByteBufs bufs;
		int offset;

		public PeekingInputStream(ByteBufs bufs) {
			this.bufs = bufs;
			offset = 0;
		}

		@Override
		public int read() throws IOException {
			if (!bufs.hasRemainingBytes(offset + 1)) throw NEED_MORE_DATA_EXCEPTION;
			return bufs.peekByte(offset++);
		}

		@Override
		public int read(byte @NotNull [] b, int off, int len) throws IOException {
			if (!bufs.hasRemainingBytes(offset + 1)) throw NEED_MORE_DATA_EXCEPTION;

			int peeked = bufs.peekTo(this.offset, b, off, len);
			this.offset += peeked;
			return peeked;
		}
	}
}
