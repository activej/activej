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

package io.activej.launchers.fs.gui;

import io.activej.common.StringFormatUtils;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.Instant;

import static io.activej.common.StringFormatUtils.formatInstant;

public final class FileView {
	private final String name;
	private final String fullName;
	private final String size;
	private final String timestamp;

	public FileView(String name, String fullName, long size, long timestamp) {
		this.name = name;
		this.fullName = fullName;
		this.size = formatSize(size);
		this.timestamp = StringFormatUtils.formatInstant(Instant.ofEpochMilli(timestamp));
	}

	public String getName() {
		return name;
	}

	public String getFullName() {
		return fullName;
	}

	public String getSize() {
		return size;
	}

	public String getTimestamp() {
		return timestamp;
	}

	private static String formatSize(long bytes) {
		long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
		if (absB < 1024) {
			return bytes + " B";
		}
		long value = absB;
		CharacterIterator ci = new StringCharacterIterator("KMGTPE");
		for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
			value >>= 10;
			ci.next();
		}
		value *= Long.signum(bytes);
		return String.format("%.1f %ciB", value / 1024.0, ci.current());
	}
}
