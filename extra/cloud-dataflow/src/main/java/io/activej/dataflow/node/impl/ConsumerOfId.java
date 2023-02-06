package io.activej.dataflow.node.impl;

import io.activej.async.function.AsyncConsumer;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.function.ConsumerEx;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.AbstractNode;
import io.activej.dataflow.node.PartitionedStreamConsumerFactory;
import io.activej.datastream.StreamConsumer;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Represents a node, which sends data items to a consumer specified by 'id'.
 */
@ExposedInternals
public final class ConsumerOfId extends AbstractNode {
	public final String id;
	public final int partitionIndex;
	public final int maxPartitions;
	public final StreamId input;

	/**
	 * Constructs a new node consumer, which sends data items from the given input stream to the specified consumer.
	 *
	 * @param id             id of output consumer
	 * @param partitionIndex index of partition where node is bound
	 * @param maxPartitions  total number of partitions
	 * @param input          id of input stream
	 */
	public ConsumerOfId(int index, String id, int partitionIndex, int maxPartitions, StreamId input) {
		super(index);
		this.id = id;
		this.partitionIndex = partitionIndex;
		this.maxPartitions = maxPartitions;
		this.input = input;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return List.of(input);
	}

	@Override
	public void createAndBind(Task task) {
		Object object = task.get(id);
		StreamConsumer<?> streamConsumer;
		if (object instanceof Collection) {
			streamConsumer = StreamConsumer.ofConsumer(((Collection<?>) object)::add);
		} else if (object instanceof Consumer) {
			streamConsumer = StreamConsumer.ofConsumer(((Consumer<?>) object)::accept);
		} else if (object instanceof ConsumerEx) {
			streamConsumer = StreamConsumer.ofConsumer((ConsumerEx<?>) object);
		} else if (object instanceof AsyncConsumer) {
			streamConsumer = StreamConsumer.ofChannelConsumer(ChannelConsumers.ofAsyncConsumer((AsyncConsumer<?>) object));
		} else if (object instanceof ChannelConsumer) {
			streamConsumer = StreamConsumer.ofChannelConsumer((ChannelConsumer<?>) object);
		} else if (object instanceof StreamConsumer) {
			streamConsumer = (StreamConsumer<?>) object;
		} else if (object instanceof PartitionedStreamConsumerFactory) {
			streamConsumer = ((PartitionedStreamConsumerFactory<?>) object).get(partitionIndex, maxPartitions);
		} else {
			throw new IllegalStateException("Object with id " + id + " is not a valid consumer of data: " + object);
		}
		task.bindChannel(input, streamConsumer);
	}

	@Override
	public String toString() {
		return "ConsumerOfId{" +
				"id='" + id + '\'' +
				", partitionIndex=" + partitionIndex +
				", maxPartitions=" + maxPartitions +
				", input=" + input +
				'}';
	}
}
