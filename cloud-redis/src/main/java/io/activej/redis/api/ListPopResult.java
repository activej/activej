package io.activej.redis.api;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;

public class ListPopResult {
	private final Charset charset;
	private final String key;
	private final byte[] result;

	public ListPopResult(Charset charset, String key, byte[] result) {
		this.charset = charset;
		this.key = key;
		this.result = result;
	}

	@NotNull
	public String getKey() {
		return key;
	}

	@NotNull
	public String getResult() {
		return new String(result, charset);
	}

	@NotNull
	public byte[] getResultAsBinary() {
		return result;
	}

	@Override
	public String toString() {
		return "ListPopResult{" +
				"key='" + key + '\'' +
				", result=" + getResult() +
				'}';
	}
}
