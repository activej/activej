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

import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public interface LogNamingScheme {
	String path(String logPartition, LogFile logFile);

	@Nullable PartitionAndFile parse(String name);

	LogFile format(long currentTimeMillis);

	String getListGlob(String logPartition);

	DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH").withZone(ZoneOffset.UTC);

	LogNamingScheme NAME_PARTITION_REMAINDER = new LogNamingScheme() {
		@Override
		public String path(String logPartition, LogFile logFile) {
			return
				logFile.getName() +
				"." +
				logPartition +
				(logFile.getRemainder() != 0 ?
					"-" + logFile.getRemainder() :
					"");
		}

		@Override
		public PartitionAndFile parse(String s) {
			int index1 = s.indexOf('.');
			if (index1 == -1)
				return null;
			String name = s.substring(0, index1);
			if (name.isEmpty())
				return null;
			s = s.substring(index1 + 1);
			int remainder = 0;
			int index2 = s.indexOf('-');
			String logPartition;
			if (index2 != -1) {
				logPartition = s.substring(0, index2);
				try {
					remainder = Integer.parseInt(s.substring(index2 + 1));
				} catch (NumberFormatException e) {
					return null;
				}
			} else {
				logPartition = s;
			}
			if (logPartition.isEmpty())
				return null;
			return new PartitionAndFile(logPartition, new LogFile(name, remainder));
		}

		@Override
		public LogFile format(long currentTimeMillis) {
			return new LogFile(
				DEFAULT_DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(currentTimeMillis)),
				(int) ((currentTimeMillis / 1000L) % 3600));
		}

		@Override
		public String getListGlob(String logPartition) {
			return "**";
		}
	};

	LogNamingScheme NAME_PARTITION_REMAINDER_SEQ = new LogNamingScheme() {
		int remainder;

		@Override
		public String path(String logPartition, LogFile logFile) {
			return NAME_PARTITION_REMAINDER.path(logPartition, logFile);
		}

		@Override
		public PartitionAndFile parse(String name) {
			return NAME_PARTITION_REMAINDER.parse(name);
		}

		@Override
		public LogFile format(long currentTimeMillis) {
			return new LogFile(NAME_PARTITION_REMAINDER.format(currentTimeMillis).getName(), remainder++);
		}

		@Override
		public String getListGlob(String logPartition) {
			return NAME_PARTITION_REMAINDER.getListGlob(logPartition);
		}
	};

	default LogNamingScheme withSuffix(String suffix) {
		LogNamingScheme self = this;
		return new LogNamingScheme() {
			@Override
			public String path(String logPartition, LogFile logFile) {
				return self.path(logPartition, logFile) + suffix;
			}

			@Override
			public PartitionAndFile parse(String name) {
				if (!name.endsWith(suffix)) return null;
				return self.parse(name.substring(0, name.length() - suffix.length()));
			}

			@Override
			public LogFile format(long currentTimeMillis) {
				return self.format(currentTimeMillis);
			}

			@Override
			public String getListGlob(String logPartition) {
				return self.getListGlob(logPartition);
			}
		};
	}

	default LogNamingScheme withPrefix(String prefix) {
		LogNamingScheme self = this;
		return new LogNamingScheme() {
			@Override
			public String path(String logPartition, LogFile logFile) {
				return prefix + self.path(logPartition, logFile);
			}

			@Override
			public PartitionAndFile parse(String name) {
				if (!name.startsWith(prefix)) return null;
				return self.parse(name.substring(prefix.length()));
			}

			@Override
			public LogFile format(long currentTimeMillis) {
				return self.format(currentTimeMillis);
			}

			@Override
			public String getListGlob(String logPartition) {
				return self.getListGlob(logPartition);
			}
		};
	}

}
