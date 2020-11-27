package io.activej.redis.api;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public final class ScanResult {
	private final Charset charset;
	private final String cursor;
	private final List<byte[]> elements;

	public ScanResult(Charset charset, String cursor, List<byte[]> elements) {
		this.charset = charset;
		this.cursor = cursor;
		this.elements = elements;
	}

	public String getCursor() {
		return cursor;
	}

	public int getCursorAsInt() {
		return Integer.parseInt(cursor);
	}

	public long getCursorAsLong() {
		return Long.parseLong(cursor);
	}

	public BigInteger getCursorAsBigInteger(){
		return new BigInteger(cursor);
	}

	public List<String> getElements() {
		List<String> result = new ArrayList<>(elements.size());
		for (byte[] element : elements) {
			result.add(new String(element, charset));
		}
		return result;
	}

	public List<byte[]> getElementsAsBinary() {
		return elements;
	}
}
