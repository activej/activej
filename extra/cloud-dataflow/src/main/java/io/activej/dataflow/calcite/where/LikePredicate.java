package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import org.jetbrains.annotations.Nullable;

import java.util.regex.Pattern;

public final class LikePredicate implements WherePredicate {
	private final Operand value;
	private final Operand pattern;

	private @Nullable CompiledPattern compiledPattern;

	public LikePredicate(@Deserialize("value") Operand value, @Deserialize("pattern") Operand pattern) {
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

	@Serialize(order = 1)
	public Operand getValue() {
		return value;
	}

	@Serialize(order = 2)
	public Operand getPattern() {
		return pattern;
	}

	private record CompiledPattern(Pattern pattern, String original) {
	}
}
