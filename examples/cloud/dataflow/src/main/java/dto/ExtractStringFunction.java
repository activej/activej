package dto;

import java.util.function.Function;

public final class ExtractStringFunction implements Function<StringCount, String> {
	@Override
	public String apply(StringCount stringCount) {
		return stringCount.string;
	}
}
