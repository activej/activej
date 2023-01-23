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

package io.activej.multilog;

import java.util.Objects;

public final class LogPosition implements Comparable<LogPosition> {
	private static final LogPosition INITIAL_LOG_POSITION = new LogPosition(new LogFile("", 0), 0);

	private final LogFile logFile;
	private final long position;

	private LogPosition(LogFile logFile, long position) {
		this.logFile = logFile;
		this.position = position;
	}

	public static LogPosition create(LogFile logFile, long position) {
		return new LogPosition(logFile, position);
	}

	public static LogPosition initial() {
		return INITIAL_LOG_POSITION;
	}

	public boolean isBeginning() {
		return position == 0L;
	}

	public boolean isInitial() {
		return INITIAL_LOG_POSITION.equals(this);
	}

	public LogFile getLogFile() {
		return logFile;
	}

	public long getPosition() {
		return position;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		LogPosition that = (LogPosition) o;
		if (position != that.position) return false;
		return Objects.equals(logFile, that.logFile);
	}

	@Override
	public int hashCode() {
		return 31 * logFile.hashCode() + (int) (position ^ (position >>> 32));
	}

	@Override
	public String toString() {
		return "LogPosition{logFile=" + logFile + ", position=" + position + '}';
	}

	@Override
	public int compareTo(LogPosition o) {
		int result = this.logFile.compareTo(o.logFile);
		return result != 0 ? result : Long.compare(this.position, o.position);
	}
}
