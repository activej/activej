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

package io.activej.dataflow.proto.serializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.dataflow.exception.DataflowException;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.StreamSchemas;
import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.node.*;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.JavaClassProto.JavaClass;
import io.activej.dataflow.proto.NodeProto;
import io.activej.dataflow.proto.NodeProto.Node.*;
import io.activej.dataflow.proto.NodeProto.Node.Download.Address;
import io.activej.dataflow.proto.NodeProto.Node.Reduce.Input;
import io.activej.dataflow.proto.NodeStatProto;
import io.activej.dataflow.proto.NodeStatProto.NodeStat.Binary;
import io.activej.dataflow.proto.NodeStatProto.NodeStat.Test;
import io.activej.dataflow.proto.StreamIdProto;
import io.activej.dataflow.proto.StreamSchemaProto;
import io.activej.dataflow.proto.StreamSchemaProto.StreamSchema.Custom;
import io.activej.dataflow.proto.StreamSchemaProto.StreamSchema.Simple;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.NodeStat;
import io.activej.dataflow.stats.TestNodeStat;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.inject.util.Constructors;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkNotNull;
import static java.util.stream.Collectors.toList;

public final class ProtobufUtils {

	public static <I extends Message, O extends Message> ByteBufsCodec<I, O> codec(Parser<I> inputParser) {
		return new ByteBufsCodec<>() {
			@Override
			public ByteBuf encode(O item) {
				byte[] bytes = item.toByteArray();

				int length = bytes.length;
				ByteBuf buf = ByteBufPool.allocate(length + 5);

				buf.writeVarInt(length);
				buf.put(bytes);
				return buf;
			}

			@Override
			public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
				return ByteBufsDecoder.ofVarIntSizePrefixedBytes()
						.andThen(buf -> {
							try {
								return inputParser.parseFrom(buf.asArray());
							} catch (InvalidProtocolBufferException e) {
								throw new MalformedDataException(e);
							}
						})
						.tryDecode(bufs);
			}
		};
	}

	public static StreamIdProto.StreamId convert(StreamId streamId) {
		return StreamIdProto.StreamId.newBuilder().setId(streamId.getId()).build();
	}

	public static StreamId convert(StreamIdProto.StreamId streamId) {
		return new StreamId(streamId.getId());
	}

	public static List<NodeProto.Node> convert(Collection<Node> nodes, FunctionSerializer functionSerializer,
			@Nullable CustomNodeSerializer customNodeSerializer,
			@Nullable CustomStreamSchemaSerializer customStreamSchemaSerializer
	) {
		return nodes.stream()
				.map(node -> convert(node, functionSerializer, customNodeSerializer, customStreamSchemaSerializer))
				.collect(toList());
	}

	public static NodeProto.Node convert(Node node, FunctionSerializer functionSerializer,
			@Nullable CustomNodeSerializer customNodeSerializer,
			@Nullable CustomStreamSchemaSerializer customStreamSchemaSerializer
	) {
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
					.setOutput(convert(reduce.getOutput())));
		} else if (node instanceof NodeUpload<?> upload) {
			builder.setUpload(NodeProto.Node.Upload.newBuilder()
					.setIndex(upload.getIndex())
					.setStreamSchema(convert(upload.getStreamSchema(), customStreamSchemaSerializer))
					.setStreamId(convert(upload.getStreamId())));
		} else if (node instanceof NodeSort<?, ?> sort) {
			builder.setSort(NodeProto.Node.Sort.newBuilder()
					.setIndex(sort.getIndex())
					.setStreamSchema(convert(sort.getStreamSchema(), customStreamSchemaSerializer))
					.setKeyFunction(functionSerializer.serializeFunction(sort.getKeyFunction()))
					.setKeyComparator(functionSerializer.serializeComparator(sort.getKeyComparator()))
					.setDeduplicate(sort.isDeduplicate())
					.setItemsInMemorySize(sort.getItemsInMemorySize())
					.setInput(convert(sort.getInput()))
					.setOutput(convert(sort.getOutput())));
		} else if (node instanceof NodeMerge<?, ?> merge) {
			builder.setMerge(NodeProto.Node.Merge.newBuilder()
					.setIndex(merge.getIndex())
					.setKeyFunction(functionSerializer.serializeFunction(merge.getKeyFunction()))
					.setKeyComparator(functionSerializer.serializeComparator(merge.getKeyComparator()))
					.setDeduplicate(merge.isDeduplicate())
					.addAllInputs(convertIds(merge.getInputs()))
					.setOutput(convert(merge.getOutput())));
		} else if (node instanceof NodeShard<?, ?> shard) {
			builder.setShard(NodeProto.Node.Shard.newBuilder()
					.setIndex(shard.getIndex())
					.setKeyFunction(functionSerializer.serializeFunction(shard.getKeyFunction()))
					.setInput(convert(shard.getInput()))
					.addAllOutputs(convertIds(shard.getOutputs()))
					.setNonce(shard.getNonce()));
		} else if (node instanceof NodeDownload<?> download) {
			builder.setDownload(Download.newBuilder()
					.setIndex(download.getIndex())
					.setStreamSchema(convert(download.getStreamSchema(), customStreamSchemaSerializer))
					.setAddress(Address.newBuilder()
							.setHost(download.getAddress().getHostName())
							.setPort(download.getAddress().getPort()))
					.setInput(convert(download.getStreamId()))
					.setOutput(convert(download.getOutput())));
		} else if (node instanceof NodeMap<?, ?> map) {
			builder.setMap(NodeProto.Node.Map.newBuilder()
					.setIndex(map.getIndex())
					.setFunction(functionSerializer.serializeFunction(map.getFunction()))
					.setInput(convert(map.getInput()))
					.setOutput(convert(map.getOutput())));
		} else if (node instanceof NodeJoin<?, ?, ?, ?> join) {
			builder.setJoin(Join.newBuilder()
					.setIndex(join.getIndex())
					.setLeft(convert(join.getLeft()))
					.setRight(convert(join.getRight()))
					.setOutput(convert(join.getOutput()))
					.setComparator(functionSerializer.serializeComparator(join.getKeyComparator()))
					.setLeftKeyFunction(functionSerializer.serializeFunction(join.getLeftKeyFunction()))
					.setRightKeyFunction(functionSerializer.serializeFunction(join.getRightKeyFunction()))
					.setJoiner(functionSerializer.serializeJoiner(join.getJoiner())));
		} else if (node instanceof NodeFilter<?> filter) {
			builder.setFilter(Filter.newBuilder()
					.setIndex(filter.getIndex())
					.setPredicate(functionSerializer.serializePredicate(filter.getPredicate()))
					.setInput(convert(filter.getInput()))
					.setOutput(convert(filter.getOutput())));
		} else if (node instanceof NodeConsumerOfId<?> consumerOfId) {
			builder.setConsumerOfId(ConsumerOfId.newBuilder()
					.setIndex(consumerOfId.getIndex())
					.setId(consumerOfId.getId())
					.setPartitionIndex(consumerOfId.getPartitionIndex())
					.setMaxPartitions(consumerOfId.getMaxPartitions())
					.setInput(convert(consumerOfId.getInput())));
		} else if (node instanceof NodeSupplierOfId<?> supplierOfId) {
			builder.setSupplierOfId(NodeProto.Node.SupplierOfId.newBuilder()
					.setIndex(supplierOfId.getIndex())
					.setId(supplierOfId.getId())
					.setPartitionIndex(supplierOfId.getPartitionIndex())
					.setMaxPartitions(supplierOfId.getMaxPartitions())
					.setOutput(convert(supplierOfId.getOutput())));
		} else if (node instanceof NodeReduceSimple<?, ?, ?, ?> reduceSimple) {
			builder.setReduceSimple(NodeProto.Node.ReduceSimple.newBuilder()
					.setIndex(reduceSimple.getIndex())
					.setKeyFunction(functionSerializer.serializeFunction(reduceSimple.getKeyFunction()))
					.setKeyComparator(functionSerializer.serializeComparator(reduceSimple.getKeyComparator()))
					.setReducer(functionSerializer.serializeReducer(reduceSimple.getReducer()))
					.addAllInputs(convertIds(reduceSimple.getInputs()))
					.setOutput(convert(reduceSimple.getOutput())));
		} else if (node instanceof NodeUnion<?> union) {
			builder.setUnion(NodeProto.Node.Union.newBuilder()
					.setIndex(union.getIndex())
					.addAllInputs(convertIds(union.getInputs()))
					.setOutput(convert(union.getOutput())));
		} else if (node instanceof NodeSupplierEmpty<?> empty) {
			builder.setEmpty(NodeProto.Node.Empty.newBuilder()
					.setIndex(empty.getIndex())
					.setOutput(convert(empty.getOutput())));
		} else if (node instanceof NodeOffsetLimit<?> offsetLimit) {
			builder.setOffsetLimit(OffsetLimit.newBuilder()
					.setIndex(offsetLimit.getIndex())
					.setOffset(offsetLimit.getOffset())
					.setLimit(offsetLimit.getLimit())
					.setInput(convert(offsetLimit.getInput()))
					.setOutput(convert(offsetLimit.getOutput())));
		} else {
			checkNotNull(customNodeSerializer, "Custom node serializer is not specified");

			ByteString serialized = encode(customNodeSerializer, node);
			builder.setCustomNode(CustomNode.newBuilder()
					.setSerializedNode(serialized));
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

	public static List<Node> convert(List<NodeProto.Node> nodes, FunctionSerializer functionSerializer,
			@Nullable CustomNodeSerializer customNodeSerializer,
			@Nullable CustomStreamSchemaSerializer customStreamSchemaSerializer
	) throws DataflowException {
		List<Node> list = new ArrayList<>();
		for (NodeProto.Node node : nodes) {
			list.add(convert(node, functionSerializer, customNodeSerializer, customStreamSchemaSerializer));
		}
		return list;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public static Node convert(NodeProto.Node node, FunctionSerializer functionSerializer,
			@Nullable CustomNodeSerializer customNodeSerializer,
			@Nullable CustomStreamSchemaSerializer customStreamSchemaSerializer
	) throws DataflowException {
		try {
			return switch (node.getNodeCase()) {
				case CONSUMER_OF_ID -> {
					ConsumerOfId coi = node.getConsumerOfId();
					yield new NodeConsumerOfId<>(
							coi.getIndex(),
							coi.getId(),
							coi.getPartitionIndex(),
							coi.getMaxPartitions(),
							convert(coi.getInput())
					);
				}
				case DOWNLOAD -> {
					Download download = node.getDownload();
					Address address = download.getAddress();
					yield new NodeDownload<>(
							download.getIndex(),
							convert(download.getStreamSchema(), customStreamSchemaSerializer),
							new InetSocketAddress(address.getHost(), address.getPort()),
							convert(download.getInput()),
							convert(download.getOutput())
					);
				}
				case FILTER -> {
					Filter filter = node.getFilter();
					yield new NodeFilter<>(
							filter.getIndex(),
							functionSerializer.deserializePredicate(filter.getPredicate()),
							convert(filter.getInput()),
							convert(filter.getOutput())
					);
				}
				case JOIN -> {
					Join join = node.getJoin();
					Comparator joinComparator = functionSerializer.deserializeComparator(join.getComparator());
					Function leftKeyFunction = functionSerializer.deserializeFunction(join.getLeftKeyFunction());
					Function rightKeyFunction = functionSerializer.deserializeFunction(join.getRightKeyFunction());
					LeftJoiner joiner = functionSerializer.deserializeJoiner(join.getJoiner());
					yield new NodeJoin<>(
							join.getIndex(),
							convert(join.getLeft()),
							convert(join.getRight()),
							convert(join.getOutput()),
							joinComparator,
							leftKeyFunction,
							rightKeyFunction,
							joiner
					);
				}
				case MAP -> {
					NodeProto.Node.Map map = node.getMap();
					yield new NodeMap<>(
							map.getIndex(),
							functionSerializer.deserializeFunction(map.getFunction()),
							convert(map.getInput()),
							convert(map.getOutput())
					);
				}
				case MERGE -> {
					Merge merge = node.getMerge();
					Function function = functionSerializer.deserializeFunction(merge.getKeyFunction());
					Comparator mergeComparator = functionSerializer.deserializeComparator(merge.getKeyComparator());
					yield new NodeMerge<>(
							merge.getIndex(),
							function,
							mergeComparator,
							merge.getDeduplicate(),
							convertProtoIds(merge.getInputsList()),
							convert(merge.getOutput())
					);
				}
				case REDUCE -> {
					Reduce reduce = node.getReduce();
					Comparator reduceComparator = functionSerializer.deserializeComparator(reduce.getKeyComparator());
					yield new NodeReduce<>(
							reduce.getIndex(),
							reduceComparator,
							convertInputs(functionSerializer, reduce.getInputsMap()),
							convert(reduce.getOutput())
					);
				}
				case REDUCE_SIMPLE -> {
					ReduceSimple reduceSimple = node.getReduceSimple();
					Function keyFunction = functionSerializer.deserializeFunction(reduceSimple.getKeyFunction());
					Comparator keyComparator = functionSerializer.deserializeComparator(reduceSimple.getKeyComparator());
					Reducer reducer = functionSerializer.deserializeReducer(reduceSimple.getReducer());
					yield new NodeReduceSimple<>(
							reduceSimple.getIndex(),
							keyFunction,
							keyComparator,
							reducer,
							convertProtoIds(reduceSimple.getInputsList()),
							convert(reduceSimple.getOutput())
					);
				}
				case SHARD -> {
					Shard shard = node.getShard();
					yield new NodeShard<>(
							shard.getIndex(),
							functionSerializer.deserializeFunction(shard.getKeyFunction()),
							convert(shard.getInput()),
							convertProtoIds(shard.getOutputsList()),
							shard.getNonce()
					);
				}
				case SORT -> {
					Sort sort = node.getSort();
					Function sortFunction = functionSerializer.deserializeFunction(sort.getKeyFunction());
					Comparator sortComparator = functionSerializer.deserializeComparator(sort.getKeyComparator());
					yield new NodeSort<>(
							sort.getIndex(),
							convert(sort.getStreamSchema(), customStreamSchemaSerializer),
							sortFunction,
							sortComparator,
							sort.getDeduplicate(),
							sort.getItemsInMemorySize(),
							convert(sort.getInput()),
							convert(sort.getOutput())
					);
				}
				case SUPPLIER_OF_ID -> {
					SupplierOfId sid = node.getSupplierOfId();
					yield new NodeSupplierOfId<>(
							sid.getIndex(),
							sid.getId(),
							sid.getPartitionIndex(),
							sid.getMaxPartitions(),
							convert(sid.getOutput())
					);
				}
				case UNION -> {
					Union union = node.getUnion();
					yield new NodeUnion<>(
							union.getIndex(),
							convertProtoIds(union.getInputsList()),
							convert(union.getOutput())
					);
				}
				case UPLOAD -> {
					Upload upload = node.getUpload();
					yield new NodeUpload<>(
							upload.getIndex(),
							convert(upload.getStreamSchema(), customStreamSchemaSerializer),
							convert(upload.getStreamId())
					);
				}
				case EMPTY -> {
					Empty empty = node.getEmpty();
					yield new NodeSupplierEmpty<>(empty.getIndex(), convert(empty.getOutput()));
				}
				case OFFSET_LIMIT -> {
					OffsetLimit offsetLimit = node.getOffsetLimit();
					yield new NodeOffsetLimit<>(
							offsetLimit.getIndex(),
							offsetLimit.getOffset(),
							offsetLimit.getLimit(),
							convert(offsetLimit.getInput()),
							convert(offsetLimit.getOutput())
					);
				}
				case CUSTOM_NODE -> {
					checkNotNull(customNodeSerializer, "Custom node serializer is not specified");
					CustomNode customNode = node.getCustomNode();
					yield decode(customNodeSerializer, customNode.getSerializedNode());
				}
				case NODE_NOT_SET -> throw new DataflowException("Node was not set");
			};
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

	private static <T> StreamSchema<T> convert(
			StreamSchemaProto.StreamSchema streamSchema,
			@Nullable CustomStreamSchemaSerializer customStreamSchemaSerializer
	) throws DataflowException {
		return switch (streamSchema.getStreamSchemaCase()) {
			case SIMPLE -> {
				Simple simple = streamSchema.getSimple();
				JavaClass javaClass = simple.getJavaClass();
				Class<T> aClass = JavaClassConverter.convert(javaClass);
				yield StreamSchemas.simple(aClass);
			}
			case CUSTOM -> {
				checkNotNull(customStreamSchemaSerializer, "Custom stream schema serializer is not specified");
				Custom custom = streamSchema.getCustom();
				//noinspection unchecked
				yield (StreamSchema<T>) decode(customStreamSchemaSerializer, custom.getSerialized());
			}
			case STREAMSCHEMA_NOT_SET -> throw new DataflowException("Stream schema was not set");
		};
	}

	public static <T> StreamSchemaProto.StreamSchema convert(
			StreamSchema<T> streamSchema,
			@Nullable CustomStreamSchemaSerializer customStreamSchemaSerializer
	) {
		StreamSchemaProto.StreamSchema.Builder builder = StreamSchemaProto.StreamSchema.newBuilder();
		if (streamSchema instanceof StreamSchemas.Simple<T>) {
			builder.setSimple(
					Simple.newBuilder()
							.setJavaClass(JavaClassConverter.convert(streamSchema.createClass()))
			);
		} else {
			checkNotNull(customStreamSchemaSerializer, "Custom stream schema serializer is not specified");

			ByteString serialized = encode(customStreamSchemaSerializer, streamSchema);
			builder.setCustom(
					Custom.newBuilder()
							.setSerialized(serialized)
			);

		}
		return builder.build();
	}

	private static <T> ByteString encode(BinarySerializer<T> serializer, T item) {
		int bufferSize = 128;

		while (true) {
			try {
				byte[] buffer = new byte[bufferSize];
				int encoded = serializer.encode(buffer, 0, item);
				return ByteString.copyFrom(buffer, 0, encoded);
			} catch (ArrayIndexOutOfBoundsException e) {
				bufferSize *= 2;
			}
		}
	}

	private static <T> T decode(BinarySerializer<T> serializer, ByteString byteString) {
		return serializer.decode(byteString.toByteArray(), 0);
	}
}
