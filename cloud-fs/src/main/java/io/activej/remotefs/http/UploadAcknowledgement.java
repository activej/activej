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

package io.activej.remotefs.http;

import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredCodecs;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.codec.StructuredCodecs.INT_CODEC;
import static io.activej.codec.StructuredCodecs.ofEnum;
import static io.activej.remotefs.http.UploadAcknowledgement.Status.ERROR;
import static io.activej.remotefs.http.UploadAcknowledgement.Status.OK;

public final class UploadAcknowledgement {
	public static final StructuredCodec<UploadAcknowledgement> CODEC = StructuredCodecs.object(UploadAcknowledgement::new,
			"status", UploadAcknowledgement::getStatus, ofEnum(Status.class),
			"errorCode", UploadAcknowledgement::getErrorCode, INT_CODEC.nullable());

	@NotNull
	private final Status status;
	@Nullable
	private final Integer errorCode;

	private UploadAcknowledgement(@NotNull Status status, @Nullable Integer errorCode) {
		this.status = status;
		this.errorCode = errorCode;
	}

	static UploadAcknowledgement ok() {
		return new UploadAcknowledgement(OK, null);
	}

	static UploadAcknowledgement ofErrorCode(int errorCode) {
		return new UploadAcknowledgement(ERROR, errorCode);
	}

	@NotNull
	public Status getStatus() {
		return status;
	}

	@Nullable
	public Integer getErrorCode() {
		return errorCode;
	}

	public enum Status {
		OK, ERROR
	}

	@Override
	public String toString() {
		return "UploadAcknowledgement{" +
				"status=" + status +
				(errorCode == null ? "" : (", errorCode=" + errorCode)) +
				'}';
	}
}
