package io.activej.dataflow.node;

import io.activej.dataflow.DataflowException;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public abstract class AbstractNode implements Node {
	private final int index;

	protected @Nullable Instant finished = null;

	protected @Nullable Exception error = null;

	public AbstractNode(int index) {
		this.index = index;
	}

	@Override
	public void finish(@Nullable Exception e) {
		if (e != null && !(e instanceof DataflowException)) {
			e = new DataflowException(e);
		}
		error = e;
		finished = Instant.now();
	}

	public @Nullable Instant getFinished() {
		return finished;
	}

	public @Nullable Exception getError() {
		return error;
	}

	@Override
	public @Nullable NodeStat getStats() {
		return null;
	}

	@Override
	public int getIndex() {
		return index;
	}
}
