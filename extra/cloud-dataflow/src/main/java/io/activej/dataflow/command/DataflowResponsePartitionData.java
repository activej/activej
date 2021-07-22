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

package io.activej.dataflow.command;

import com.dslplatform.json.CompiledJson;
import io.activej.dataflow.graph.TaskStatus;

import java.util.List;

@CompiledJson
public class DataflowResponsePartitionData extends DataflowResponse {
	private final int running;
	private final int succeeded;
	private final int failed;
	private final int canceled;
	private final List<TaskDesc> last;

	public DataflowResponsePartitionData(int running, int succeeded, int failed, int canceled, List<TaskDesc> last) {
		this.running = running;
		this.succeeded = succeeded;
		this.failed = failed;
		this.canceled = canceled;
		this.last = last;
	}

	public int getRunning() {
		return running;
	}

	public int getSucceeded() {
		return succeeded;
	}

	public int getFailed() {
		return failed;
	}

	public int getCanceled() {
		return canceled;
	}

	public List<TaskDesc> getLast() {
		return last;
	}

	@Override
	public String toString() {
		return "DataflowResponsePartitionData{" +
				"succeeded=" + succeeded +
				", failed=" + failed +
				", canceled=" + canceled +
				", running=" + running +
				", last=" + last +
				'}';
	}

	@CompiledJson
	public static final class TaskDesc {
		private final long id;
		private final TaskStatus status;

		public TaskDesc(long id, TaskStatus status) {
			this.id = id;
			this.status = status;
		}

		public long getId() {
			return id;
		}

		public TaskStatus getStatus() {
			return status;
		}

		@Override
		public String toString() {
			return "TaskDesc{" +
					"id=" + id +
					", status=" + status +
					'}';
		}
	}
}
