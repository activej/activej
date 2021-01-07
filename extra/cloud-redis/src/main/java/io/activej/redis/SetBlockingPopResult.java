/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.redis;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;

public class SetBlockingPopResult {
	private final Charset charset;
	private final String key;
	private final byte[] result;
	private final double score;

	SetBlockingPopResult(Charset charset, String key, byte[] result, double score) {
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
