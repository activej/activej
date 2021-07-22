package io.activej.ot.utils;

import com.dslplatform.json.CompiledJson;

@CompiledJson
public interface TestOp {
	int apply(int prev);
}
