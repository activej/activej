package io.activej.redis;

import org.jetbrains.annotations.Nullable;

import java.util.*;

final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	@SafeVarargs
	static <T> List<T> list(T main, T... otherArguments) {
		List<T> result = new ArrayList<>(otherArguments.length + 1);
		result.add(main);
		Collections.addAll(result, otherArguments);
		return result;
	}

	@SafeVarargs
	static <T> List<T> list(T first, T second, T... otherArguments) {
		List<T> result = new ArrayList<>(otherArguments.length + 2);
		result.add(first);
		result.add(second);
		Collections.addAll(result, otherArguments);
		return result;
	}

	@SafeVarargs
	static <T> List<T> list(T first, T second, T third, T... otherArguments) {
		List<T> result = new ArrayList<>(otherArguments.length + 3);
		result.add(first);
		result.add(second);
		result.add(third);
		Collections.addAll(result, otherArguments);
		return result;
	}

	static int deepHashCode(List<?> array) {
		int result = 1;
		for (Object o : array) {
			if (o == null) result = 31 * result + 1;
			else if (o instanceof byte[]) result = 31 * result + Arrays.hashCode((byte[]) o);
			else if (o instanceof List) result = 31 * result + deepHashCode((List<?>) o);
			else result = 31 * result + o.hashCode();
		}
		return result;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	static boolean deepEquals(@Nullable List<?> thisArray, @Nullable List<?> thatArray) {
		if (thisArray == thatArray) return true;
		if (thisArray == null || thatArray == null) return false;

		if (thisArray.size() != thatArray.size()) return false;

		Iterator<?> thisIterator = thisArray.iterator();
		Iterator<?> thatIterator = thatArray.iterator();

		while (thisIterator.hasNext()) {
			Object nextThis = thisIterator.next();
			Object nextThat = thatIterator.next();

			if (nextThis == nextThat) continue;
			if (nextThis == null || nextThat == null) return false;

			if (nextThis instanceof List && nextThat instanceof List) {
				if (!deepEquals((List<?>) nextThis, (List<?>) nextThat)) return false;
				continue;
			}

			if (nextThis.getClass() != nextThat.getClass()) return false;

			if (nextThis instanceof byte[]) {
				if (!Arrays.equals((byte[]) nextThis, (byte[]) nextThat)) return false;
				continue;
			}


			if (!nextThis.equals(nextThat)) return false;
		}

		return !thatIterator.hasNext();
	}
}
