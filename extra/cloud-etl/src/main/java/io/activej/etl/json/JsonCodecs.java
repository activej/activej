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

package io.activej.etl.json;

import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.json.JsonCodec;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;

import java.util.Map;

import static io.activej.common.collection.CollectorUtils.toLinkedHashMap;
import static io.activej.json.JsonCodecs.*;

public final class JsonCodecs {

	public static <D> JsonCodec<LogDiff<D>> ofLogDiff(JsonCodec<D> diffCodec) {
		return ofObject(LogDiff::of,
			"positions", LogDiff::getPositions, ofLogPositions(),
			"ops", LogDiff::getDiffs, ofList(diffCodec));
	}

	private record LogPositionEntry(String key, LogPosition from, LogPosition to) {}

	public static JsonCodec<Map<String, LogPositionDiff>> ofLogPositions() {
		return ofList(
				ofObject(LogPositionEntry::new,
					"log", LogPositionEntry::key, ofString(),
					"from", LogPositionEntry::from, ofLogPosition(),
					"to", LogPositionEntry::to, ofLogPosition()
				))
			.transform(
				map -> map.entrySet().stream().map(entry -> new LogPositionEntry(entry.getKey(), entry.getValue().from(), entry.getValue().to())).toList(),
				logPositionEntries -> logPositionEntries.stream().collect(toLinkedHashMap(LogPositionEntry::key, logPositionEntry -> new LogPositionDiff(logPositionEntry.from, logPositionEntry.to))));
	}

	public static JsonCodec<LogPosition> ofLogPosition() {
		return ofArrayObject(
				ofString(), ofInteger(), ofLong()
			)
			.transform(
				logPosition -> new Object[]{logPosition.getLogFile().getName(), logPosition.getLogFile().getRemainder(), logPosition.getPosition()},
				array -> LogPosition.create(new LogFile((String) array[0], (int) array[1]), (long) array[2]));
	}

}
