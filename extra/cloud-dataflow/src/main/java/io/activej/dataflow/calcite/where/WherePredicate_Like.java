package io.activej.dataflow.calcite.where;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.regex.Pattern;

public final class WherePredicate_Like implements WherePredicate {
	private final Operand<?> value;
	private final Operand<?> pattern;

	private @Nullable CompiledPattern compiledPattern;

	public WherePredicate_Like(Operand<?> value, Operand<?> pattern) {
		this.value = value;
		this.pattern = pattern;
	}

	@Override
	public boolean test(Record record) {
		String patternValue = pattern.getValue(record);
		if (patternValue == null) return false;

		if (compiledPattern == null || !compiledPattern.original.equals(patternValue)) {
			Pattern compiled = Pattern.compile("^" + patternValue.replaceAll("%", ".*").replaceAll("_", ".") + "$");
			compiledPattern = new CompiledPattern(compiled, patternValue);
		}

		String value = this.value.getValue(record);
		if (value == null) return false;

		return compiledPattern.pattern.matcher(value).matches();
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new WherePredicate_Like(
				value.materialize(params),
				pattern.materialize(params)
		);
	}

	public Operand<?> getValue() {
		return value;
	}

	public Operand<?> getPattern() {
		return pattern;
	}

	public record CompiledPattern(Pattern pattern, String original) {
	}

	@Override
	public String toString() {
		return "LikePredicate[" +
				"value=" + value +
				", pattern=" + pattern +
				']';
	}
}
