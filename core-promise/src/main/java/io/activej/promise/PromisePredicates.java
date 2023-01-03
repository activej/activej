package io.activej.promise;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

@SuppressWarnings("unchecked")
public class PromisePredicates {
	private static final BiPredicate<?, Exception> IS_COMPLETE = (t, e) -> true;
	private static final BiPredicate<?, Exception> IS_RESULT = (t, e) -> e == null;
	private static final BiPredicate<?, Exception> IS_EXCEPTION = (t, e) -> e != null;

	public static <T> BiPredicate<? super T, Exception> isComplete() {
		return (BiPredicate<? super T, Exception>) IS_COMPLETE;
	}

	public static <T> BiPredicate<? super T, Exception> isResult() {
		return (BiPredicate<? super T, Exception>) IS_RESULT;
	}

	public static <T> BiPredicate<? super T, Exception> isResult(Predicate<? super T> predicate) {
		return (t, e) -> e == null && predicate.test(t);
	}

	public static <T> BiPredicate<? super T, Exception> isResultOrException(Predicate<? super T> predicate) {
		return (t, e) -> e != null || predicate.test(t);
	}

	public static <T> BiPredicate<? super T, Exception> isException() {
		return (BiPredicate<? super T, Exception>) IS_EXCEPTION;
	}

	public static <T> BiPredicate<? super T, Exception> isException(Predicate<Exception> predicate) {
		return (t, e) -> e != null && predicate.test(e);
	}

	public static <T> BiPredicate<? super T, Exception> isException(Class<? extends Exception> errorClass) {
		return isException(e -> errorClass.isAssignableFrom(e.getClass()));
	}
}
