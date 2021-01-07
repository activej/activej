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

package io.activej.etl;

import io.activej.codec.*;
import io.activej.common.exception.MalformedDataException;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.codec.StructuredCodecs.STRING_CODEC;
import static io.activej.codec.json.JsonUtils.oneline;

public final class LogDiffCodec<D> implements StructuredCodec<LogDiff<D>> {
	public static final String POSITIONS = "positions";
	public static final String LOG = "log";
	public static final String FROM = "from";
	public static final String TO = "to";
	public static final String OPS = "ops";

	public static final StructuredCodec<LogPosition> LOG_POSITION_CODEC = oneline(new LogPositionCodec());

	public static final class LogPositionCodec implements StructuredCodec<LogPosition> {
		@Override
		public void encode(StructuredOutput out, LogPosition logPosition) {
			out.writeTuple(() -> {
				out.writeString(logPosition.getLogFile().getName());
				out.writeInt(logPosition.getLogFile().getRemainder());
				out.writeLong(logPosition.getPosition());
			});
		}

		@Override
		public LogPosition decode(StructuredInput in) throws MalformedDataException {
			return in.readTuple($ -> {
				String name = in.readString();
				int n = in.readInt();
				long position = in.readLong();
				return LogPosition.create(new LogFile(name, n), position);
			});
		}
	}

	private final StructuredCodec<List<D>> opsCodec;

	private LogDiffCodec(StructuredCodec<List<D>> opsCodec) {
		this.opsCodec = opsCodec;
	}

	public static <D> LogDiffCodec<D> create(StructuredCodec<D> opCodec) {
		return new LogDiffCodec<>(StructuredCodecs.ofList(opCodec));
	}

	@Override
	public void encode(StructuredOutput out, LogDiff<D> multilogDiff) {
		out.writeObject(() -> {
			out.writeKey(POSITIONS, StructuredEncoder.ofTuple((out1, multilogDiff1) -> {
				for (Map.Entry<String, LogPositionDiff> entry : multilogDiff1.getPositions().entrySet()) {
					out1.writeObject(() -> {
						out1.writeKey(LOG, STRING_CODEC, entry.getKey());
						out1.writeKey(FROM, LOG_POSITION_CODEC, entry.getValue().from);
						out1.writeKey(TO, LOG_POSITION_CODEC, entry.getValue().to);
					});
				}
			}), multilogDiff);
			out.writeKey(OPS, opsCodec, multilogDiff.getDiffs());
		});
	}

	@Override
	public LogDiff<D> decode(StructuredInput in) throws MalformedDataException {
		return in.readObject($ -> LogDiff.of(
				in.readKey(POSITIONS, StructuredDecoder.ofTuple(in1 -> {
					Map<String, LogPositionDiff> positions = new LinkedHashMap<>();
					while (in1.hasNext()) {
						in1.readObject(() -> {
							String log = in1.readKey(LOG, STRING_CODEC);
							LogPosition from = in1.readKey(FROM, LOG_POSITION_CODEC);
							LogPosition to = in1.readKey(TO, LOG_POSITION_CODEC);
							positions.put(log, new LogPositionDiff(from, to));
						});
					}
					return positions;
				})),
				in.readKey(OPS, opsCodec)));
	}

}
