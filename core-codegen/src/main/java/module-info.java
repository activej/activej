module io.activej.codegen {
	requires transitive org.objectweb.asm;
	requires transitive org.objectweb.asm.commons;
	requires transitive io.activej.common;
	requires transitive io.activej.types;

	exports io.activej.codegen;
	exports io.activej.codegen.expression;
	exports io.activej.codegen.expression.impl;
	exports io.activej.codegen.operation;
	exports io.activej.codegen.util;
	exports io.activej.record;
}
