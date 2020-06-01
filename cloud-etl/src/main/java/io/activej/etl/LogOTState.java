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

import io.activej.multilog.LogPosition;
import io.activej.ot.OTState;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public final class LogOTState<D> implements OTState<LogDiff<D>> {
	private final Map<String, LogPosition> positions = new HashMap<>();
	private final OTState<D> dataState;

	private LogOTState(OTState<D> dataState) {
		this.dataState = dataState;
	}

	public static <D> LogOTState<D> create(OTState<D> dataState) {
		return new LogOTState<>(dataState);
	}

	public Map<String, LogPosition> getPositions() {
		return unmodifiableMap(positions);
	}

	public OTState<D> getDataState() {
		return dataState;
	}

	@Override
	public void init() {
		positions.clear();
		dataState.init();
	}

	@Override
	public void apply(LogDiff<D> op) {
		for (String log : op.getPositions().keySet()) {
			LogPositionDiff positionDiff = op.getPositions().get(log);
			positions.put(log, positionDiff.to);
		}
		for (D d : op.getDiffs()) {
			dataState.apply(d);
		}
	}

	@Override
	public String toString() {
		return "LogOTState{" +
				"positions=" + positions +
				", dataState=" + dataState +
				'}';
	}
}
