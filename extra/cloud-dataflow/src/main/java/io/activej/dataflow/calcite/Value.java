package io.activej.dataflow.calcite;

import org.apache.calcite.rex.RexDynamicParam;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;

import static io.activej.common.Checks.checkState;

@SuppressWarnings("ConstantConditions")
@ApiStatus.Internal
public class Value {
	private final @Nullable RexDynamicParam dynamicParam;
	private final @Nullable Object value;

	private Value(@Nullable Object value, @Nullable RexDynamicParam dynamicParam) {
		this.value = value;
		this.dynamicParam = dynamicParam;
	}

	public static Value materializedValue(@Nullable Object value) {
		return new Value(value, null);
	}

	public static Value unmaterializedValue(RexDynamicParam dynamicParam) {
		return new Value(null, dynamicParam);
	}

	public Value materialize(List<Object> params) {
		if (isMaterialized()) return this;

		return Value.materializedValue(params.get(dynamicParam.getIndex()));
	}

	public @Nullable Object getValue() {
		checkState(isMaterialized());

		return value;
	}

	public RexDynamicParam getDynamicParam() {
		checkState(!isMaterialized());

		return dynamicParam;
	}

	public boolean isMaterialized() {
		return dynamicParam == null;
	}

	@Override
	public String toString() {
		return isMaterialized() ? Objects.toString(value) : "<UNKNOWN(" + dynamicParam.getIndex() + ")>";
	}
}
