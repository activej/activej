package io.activej.redis.api;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;

public class SetBlockingPopResult {
	private final Charset charset;
	private final String key;
	private final byte[] result;
	private final double score;

	public SetBlockingPopResult(Charset charset, String key, byte[] result, double score) {
		this.charset = charset;
		this.key = key;
		this.result = result;
		this.score = score;
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

	public double getScore() {
		return score;
	}

	@Override
	public String toString() {
		return "SetPopResult{" +
				"key='" + key + '\'' +
				", result=" + getResult() +
				", score=" + score +
				'}';
	}
}
