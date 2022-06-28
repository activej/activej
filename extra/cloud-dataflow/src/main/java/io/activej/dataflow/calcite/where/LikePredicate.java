package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.regex.Pattern;

public final class LikePredicate implements WherePredicate {
	private final Operand<String> value;
	private final String pattern;

	private final Pattern matchPattern;

	public LikePredicate(@Deserialize("value") Operand<String> value, @Deserialize("pattern") String pattern) {
		this.value = value;
		this.pattern = pattern;

		this.matchPattern = Pattern.compile("^" + pattern.replaceAll("%", ".*").replaceAll("_", ".") + "$");
	}

	@Override
	public boolean test(Record record) {
		String value = this.value.getValue(record);

		return matchPattern.matcher(value).matches();
	}

	@Serialize(order = 1)
	public Operand<String> getValue() {
		return value;
	}

	@Serialize(order = 2)
	public String getPattern() {
		return pattern;
	}
}
