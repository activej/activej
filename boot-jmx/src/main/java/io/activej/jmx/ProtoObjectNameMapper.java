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

package io.activej.jmx;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.UnaryOperator;

import static io.activej.jmx.Utils.doMap;

public interface ProtoObjectNameMapper {

	ProtoObjectName apply(ProtoObjectName protoObjectName);

	static ProtoObjectNameMapper identity() {
		return protoObjectName -> protoObjectName;
	}

	static ProtoObjectNameMapper mapClassName(UnaryOperator<@NotNull String> mapper) {
		return doMap(ProtoObjectName::getClassName, mapper, ProtoObjectName::withClassName);
	}

	static ProtoObjectNameMapper mapPackageName(UnaryOperator<@NotNull String> mapper) {
		return doMap(ProtoObjectName::getPackageName, mapper, ProtoObjectName::withPackageName);
	}

	static ProtoObjectNameMapper mapQualifier(UnaryOperator<@NotNull Object> mapper) {
		return doMap(ProtoObjectName::getQualifier, mapper, ProtoObjectName::withQualifier);
	}

	static ProtoObjectNameMapper mapScope(UnaryOperator<@NotNull String> mapper) {
		return doMap(ProtoObjectName::getScope, mapper, ProtoObjectName::withScope);
	}

	static ProtoObjectNameMapper mapWorkerPoolQualifier(UnaryOperator<@NotNull String> mapper) {
		return doMap(ProtoObjectName::getWorkerPoolQualifier, mapper, ProtoObjectName::withWorkerPoolQualifier);
	}

	static ProtoObjectNameMapper mapWorkerId(UnaryOperator<@NotNull String> mapper) {
		return doMap(ProtoObjectName::getWorkerId, mapper, ProtoObjectName::withWorkerId);
	}

	static ProtoObjectNameMapper mapGenericParameters(UnaryOperator<@NotNull List<String>> mapper) {
		return doMap(ProtoObjectName::getGenericParameters, mapper, ProtoObjectName::withGenericParameters);
	}

	default ProtoObjectNameMapper then(ProtoObjectNameMapper next) {
		return protoObjectName -> next
				.apply(apply(protoObjectName));
	}
}
