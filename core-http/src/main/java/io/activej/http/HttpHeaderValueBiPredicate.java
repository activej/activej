package io.activej.http;

@FunctionalInterface
public interface HttpHeaderValueBiPredicate {
	boolean test(HttpHeader header, HttpHeaderValue headerValue) throws MalformedHttpException;
}
