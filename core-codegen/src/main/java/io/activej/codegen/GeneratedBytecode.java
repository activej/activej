package io.activej.codegen;

public class GeneratedBytecode {
	private final String className;
	private final byte[] bytecode;

	public GeneratedBytecode(String name, byte[] bytecode) {
		className = name;
		this.bytecode = bytecode;
	}

	public final String getClassName() {
		return className;
	}

	public final byte[] getBytecode() {
		return bytecode;
	}

	public final Class<?> defineClass(DefiningClassLoader classLoader) {
		try {
			Class<?> aClass = classLoader.defineClass(className, bytecode);
			onDefinedClass(aClass);
			return aClass;
		} catch (Exception e) {
			onError(e);
			throw e;
		}
	}

	protected void onDefinedClass(Class<?> clazz) {
	}

	protected void onError(Exception e) {
	}
}
