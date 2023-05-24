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

package io.activej.cube.exception;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class StateFarAheadException extends Exception {
	private final long startingRevision;
	private final Set<Long> missingRevisions;

	public StateFarAheadException(long startingRevision, Set<Long> missingRevisions) {
		this.startingRevision = startingRevision;
		this.missingRevisions = new HashSet<>(missingRevisions);
	}

	public long getStartingRevision() {
		return startingRevision;
	}

	public Set<Long> getMissingRevisions() {
		return Collections.unmodifiableSet(missingRevisions);
	}

	@Override
	public String getMessage() {
		return "State is far ahead of starting revision " + startingRevision +
			   ", missing revisions: " + missingRevisions;
	}
}
