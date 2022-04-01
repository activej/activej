package io.activej.dataflow.node;

import io.activej.async.function.AsyncConsumer;
import io.activej.common.function.ConsumerEx;
import io.activej.csp.ChannelConsumer;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.datastream.StreamConsumer;

import java.util.Collection;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which sends data items to a consumer specified by 'id'.
 *
 * @param <T> data items type
 */
public final class NodeConsumerOfId<T> extends AbstractNode {
	private final String id;
	private final int partitionIndex;
	private final int maxPartitions;
	private final StreamId input;

	/**
	 * Constructs a new node consumer, which sends data items from the given input stream to the specified consumer.
	 *
	 * @param id             id of output consumer
	 * @param partitionIndex index of partition where node is bound
	 * @param maxPartitions  total number of partitions
	 * @param input          id of input stream
	 */
	public NodeConsumerOfId(int index, String id, int partitionIndex, int maxPartitions, StreamId input) {
		super(index);
		this.id = id;
		this.partitionIndex = partitionIndex;
		this.maxPartitions = maxPartitions;
		this.input = input;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return singletonList(input);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(Task task) {
		Object object = task.get(id);
		StreamConsumer<T> streamConsumer;
		if (object instanceof Collection) {
			streamConsumer = StreamConsumer.ofConsumer(((Collection<T>) object)::add);
		} else if (object instanceof Consumer) {
			streamConsumer = StreamConsumer.ofConsumer(((Consumer<T>) object)::accept);
		} else if (object instanceof ConsumerEx) {
			streamConsumer = StreamConsumer.ofConsumer((ConsumerEx<T>) object);
		} else if (object instanceof AsyncConsumer) {
			streamConsumer = StreamConsumer.ofChannelConsumer(ChannelConsumer.of((AsyncConsumer<T>) object));
		} else if (object instanceof ChannelConsumer) {
			streamConsumer = StreamConsumer.ofChannelConsumer((ChannelConsumer<T>) object);
		} else if (object instanceof StreamConsumer) {
			streamConsumer = (StreamConsumer<T>) object;
		} else if (object instanceof PartitionedStreamConsumerFactory) {
			streamConsumer = ((PartitionedStreamConsumerFactory<T>) object).get(partitionIndex, maxPartitions);
		} else {
			throw new IllegalStateException("Object with id " + id + " is not a valid consumer of data: " + object);
		}
		task.bindChannel(input, streamConsumer);
	}

	public String getId() {
		return id;
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}

	public int getMaxPartitions() {
		return maxPartitions;
	}

	public StreamId getInput() {
		return input;
	}

	@Override
	public String toString() {
		return "NodeConsumerOfId{" +
				"id='" + id + '\'' +
				", partitionIndex=" + partitionIndex +
				", maxPartitions=" + maxPartitions +
				", input=" + input +
				'}';
	}
}
