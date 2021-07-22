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

package io.activej.fs.http;

import com.dslplatform.json.CompiledJson;
import io.activej.fs.exception.FsException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class UploadAcknowledgement {
	@Nullable
	private final FsException error;

	@CompiledJson
	public UploadAcknowledgement(@Nullable FsException error) {
		this.error = error;
	}

	static UploadAcknowledgement ok() {
		return new UploadAcknowledgement(null);
	}

	static UploadAcknowledgement ofError(@NotNull FsException error) {
		return new UploadAcknowledgement(error);
	}

	public boolean isOk() {
		return error == null;
	}

	@Nullable
	public FsException getError() {
		return error;
	}

	@Override
	public String toString() {
		return "UploadAcknowledgement{" +
				(error == null ? "" : ("error=" + error)) +
				'}';
	}
}
