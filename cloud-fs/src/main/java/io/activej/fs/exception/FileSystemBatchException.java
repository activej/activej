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

package io.activej.fs.exception;

import java.util.Map;

import static java.util.stream.Collectors.joining;

public final class FileSystemBatchException extends FileSystemStateException {
	private final Map<String, FileSystemScalarException> exceptions;

	public FileSystemBatchException(Map<String, FileSystemScalarException> exceptions) {
		super("Operation failed");
		this.exceptions = exceptions;
	}

	FileSystemBatchException(Map<String, FileSystemScalarException> exceptions, boolean withStack) {
		super("Operation failed", withStack);
		this.exceptions = exceptions;
	}

	public Map<String, FileSystemScalarException> getExceptions() {
		return exceptions;
	}

	@Override
	public String getMessage() {
		return super.getMessage() + " : exceptions=" + exceptions.entrySet().stream()
			.limit(10)
			.map(element -> element.getKey() + '=' + element.getValue().getMessage())
			.collect(joining(",", "{", exceptions.size() <= 10 ? "}" : ", ..and " + (exceptions.size() - 10) + " more}"));
	}
}
