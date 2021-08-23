package io.activej.codegen;

import java.util.Optional;

public interface BytecodeStorage {
	Optional<byte[]> loadBytecode(String className);

	void saveBytecode(String className, byte[] bytecode);
}
