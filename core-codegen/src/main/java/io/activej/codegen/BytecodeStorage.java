package io.activej.codegen;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public interface BytecodeStorage {
	@Nullable
	byte[] loadBytecode(String className) throws IOException;

	void saveBytecode(String className, byte[] bytecode) throws IOException;
}
