package io.activej.dataflow.node;

import io.activej.dataflow.stats.NodeStat;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.function.Function;

public abstract class AbstractNode implements Node {
	private final int index;

	@Nullable
	protected Instant finished = null;

	@Nullable
	protected Throwable error = null;

	public AbstractNode(int index) {
		this.index = index;
	}

	@Override
	public void finish(@Nullable Throwable e) {
		error = e;
		finished = Instant.now();
	}

	@Nullable
	public Instant getFinished() {
		return finished;
	}

	@Nullable
	public Throwable getError() {
		return error;
	}

	@Override
	@Nullable
	public NodeStat getStats() {
		return null;
	}

	@Override
	public int getIndex() {
		return index;
	}
}
