package io.activej.http;

@FunctionalInterface
public interface HttpHeaderValuePredicate {
	boolean test(HttpHeaderValue headerValue) throws MalformedHttpException;
}
