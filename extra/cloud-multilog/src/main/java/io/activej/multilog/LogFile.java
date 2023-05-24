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

public final class LogFile implements Comparable<LogFile> {
	private final String name;
	private final int remainder;

	public LogFile(String name, int remainder) {
		this.name = name;
		this.remainder = remainder;
	}

	@Override
	public int compareTo(LogFile o) {
		int i = name.compareTo(o.name);
		if (i != 0)
			return i;
		return Integer.compare(remainder, o.remainder);
	}

	public String getName() {
		return name;
	}

	public int getRemainder() {
		return remainder;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		LogFile that = (LogFile) o;
		return this.remainder == that.remainder &&
			   this.name.equals(that.name);
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + remainder;
		return result;
	}

	@Override
	public String toString() {
		return name + (remainder == 0 ? "" : "_" + remainder);
	}
}
