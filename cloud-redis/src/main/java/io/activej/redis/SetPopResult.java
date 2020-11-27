package io.activej.redis;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;

public class SetPopResult {
	private final Charset charset;
	private final byte[] result;
	private final double score;

	SetPopResult(Charset charset, byte[] result, double score) {
		this.charset = charset;
		this.result = result;
		this.score = score;
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
				"result=" + getResult() +
				", score=" + score +
				'}';
	}
}
