package dto;

import java.util.function.Function;

public final class CreateStringCountFunction implements Function<String, StringCount> {
	@Override
	public StringCount apply(String s) {
		return new StringCount(s, 1);
	}
}
