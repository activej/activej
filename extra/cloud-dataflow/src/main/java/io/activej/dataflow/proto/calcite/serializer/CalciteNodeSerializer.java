package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.node.FilterableNodeSupplierOfId;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.proto.calcite.CalciteNodeProto;
import io.activej.dataflow.proto.serializer.CustomNodeSerializer;
import io.activej.dataflow.proto.serializer.ProtobufUtils;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.CorruptedDataException;

import static io.activej.serializer.BinarySerializers.BYTES_SERIALIZER;

public final class CalciteNodeSerializer implements CustomNodeSerializer {
	private final DefiningClassLoader classLoader;

	public CalciteNodeSerializer(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public CalciteNodeSerializer() {
		this.classLoader = DefiningClassLoader.create();
	}

	@Override
	public void encode(BinaryOutput out, Node node) {
		byte[] item = convert(node).toByteArray();
		BYTES_SERIALIZER.encode(out, item);
	}

	@Override
	public Node decode(BinaryInput in) throws CorruptedDataException {
		byte[] serialized = BYTES_SERIALIZER.decode(in);
		CalciteNodeProto.CalciteNode calciteNode;
		try {
			calciteNode = CalciteNodeProto.CalciteNode.parseFrom(serialized);
		} catch (InvalidProtocolBufferException e) {
			throw new CorruptedDataException(e.getMessage());
		}

		return convert(calciteNode);
	}

	public Node convert(CalciteNodeProto.CalciteNode node) {
		return switch (node.getNodeCase()) {
			case FILTERABLE_SUPPLIER_OF_ID -> {
				CalciteNodeProto.CalciteNode.FilterableSupplierOfId filterableSupplierOfId = node.getFilterableSupplierOfId();
				yield new FilterableNodeSupplierOfId<>(
						filterableSupplierOfId.getIndex(),
						filterableSupplierOfId.getId(),
						WherePredicateSerializer.convert(filterableSupplierOfId.getPredicate(), classLoader),
						filterableSupplierOfId.getPartitionIndex(),
						filterableSupplierOfId.getMaxPartitions(),
						ProtobufUtils.convert(filterableSupplierOfId.getOutput())
				);
			}
			case NODE_NOT_SET -> throw new CorruptedDataException("Calcite node was not set");
		};
	}

	public static CalciteNodeProto.CalciteNode convert(Node node) {
		CalciteNodeProto.CalciteNode.Builder builder = CalciteNodeProto.CalciteNode.newBuilder();

		if (node instanceof FilterableNodeSupplierOfId<?> filterableNodeSupplierOfId) {
			builder.setFilterableSupplierOfId(
					CalciteNodeProto.CalciteNode.FilterableSupplierOfId.newBuilder()
							.setIndex(filterableNodeSupplierOfId.getIndex())
							.setId(filterableNodeSupplierOfId.getId())
							.setPredicate(WherePredicateSerializer.convert(filterableNodeSupplierOfId.getPredicate()))
							.setPartitionIndex(filterableNodeSupplierOfId.getPartitionIndex())
							.setMaxPartitions(filterableNodeSupplierOfId.getMaxPartitions())
							.setOutput(ProtobufUtils.convert(filterableNodeSupplierOfId.getOutput()))
			);
		} else {
			throw new IllegalArgumentException("Unsupported calcite node type: " + node.getClass().getName());
		}

		return builder.build();
	}
}
