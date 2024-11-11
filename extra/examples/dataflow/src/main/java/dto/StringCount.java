package dto;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public final class StringCount {

	@Serialize
	public final String string;
	@Serialize
	public int count;

	public StringCount(@Deserialize("string") String string, @Deserialize("count") int count) {
		this.string = string;
		this.count = count;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof StringCount that)) return false;
		return string.equals(that.string) && count == that.count;
	}

	@Override
	public int hashCode() {
		return 31 * string.hashCode() + count;
	}

	@Override
	public String toString() {
		return "dto.StringCount{string='" + string + '\'' + ", count=" + count + '}';
	}
}
