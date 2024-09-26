package io.activej.codegen;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static io.activej.codegen.expression.Expressions.value;
import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
public class DefiningClassLoaderTest {

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Parameter()
	public String testName;

	@Parameter(1)
	public Supplier<DefiningClassLoader> classLoaderFactory;

	private DefiningClassLoader classLoader;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return List.of(
			new Object[]{"No bytecode storage", (Supplier<DefiningClassLoader>) DefiningClassLoader::create},
			new Object[]{"File bytecode storage", (Supplier<DefiningClassLoader>) () -> {
				try {
					return DefiningClassLoader.builder()
						.withBytecodeStorage(FileBytecodeStorage.create(temporaryFolder.newFolder().toPath()))
						.build();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}}
		);
	}

	@Before
	public void setUp() {
		classLoader = classLoaderFactory.get();
	}

	@Test
	public void ensureSameClassName() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		String testString = "test string";
		String className = "io.activej.codegen.TestSupplier";
		Class<Supplier> supplier1Class = classLoader.ensureClass(className,
			() -> ClassGenerator.builder(Supplier.class)
				.withMethod("get", value(testString))
				.build());

		Class<Supplier> supplier2Class = classLoader.ensureClass(className, failingSupplier());

		assertSame(supplier1Class, supplier2Class);
		assertEquals(className, supplier1Class.getName());

		assertEquals(testString, supplier1Class.getConstructor().newInstance().get());
	}

	@Test
	public void ensureDifferentClassNames() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		String testString = "test string";
		String className1 = "io.activej.codegen.TestSupplier";
		String className2 = "MySupplier";

		Supplier<ClassGenerator<Supplier>> cbSupplier = () -> ClassGenerator.builder(Supplier.class)
			.withMethod("get", value(testString))
			.build();

		Class<Supplier> supplier1Class = classLoader.ensureClass(className1, cbSupplier);
		Class<Supplier> supplier2Class = classLoader.ensureClass(className2, cbSupplier);

		assertNotEquals(supplier1Class, supplier2Class);
		assertEquals(className1, supplier1Class.getName());
		assertEquals(className2, supplier2Class.getName());

		assertEquals(testString, supplier1Class.getConstructor().newInstance().get());
		assertEquals(testString, supplier2Class.getConstructor().newInstance().get());
	}

	@Test
	public void ensureEmptyClassName() {
		String testString = "test string";
		String className = "";

		assertThrows(ClassFormatError.class, () -> classLoader.ensureClass(className,
			() -> ClassGenerator.builder(Supplier.class)
				.withMethod("get", value(testString))
				.build()));
	}

	@Test
	public void ensureIllegalClassName() {
		String testString = "test string";
		String className = "/";

		NoClassDefFoundError e = assertThrows(NoClassDefFoundError.class, () -> classLoader.ensureClass(className,
			() -> ClassGenerator.builder(Supplier.class)
				.withMethod("get", value(testString))
				.build()));
		assertTrue(e.getMessage().startsWith("IllegalName"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void classShouldBeLoadedFromClassPathIfPossible() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		String className = "io.activej.codegen.DefiningClassLoaderTest$MySupplier";

		Class<Supplier> cls = classLoader.ensureClass(className, failingSupplier());

		Supplier<String> supplier = cls.getConstructor().newInstance();

		assertEquals(MySupplier.FROM_CLASS_PATH, supplier.get());
		assertEquals(MySupplier.class, cls);
	}

	private Supplier<ClassGenerator<Supplier>> failingSupplier() {
		return () -> {
			throw new AssertionError();
		};
	}

	public static final class MySupplier implements Supplier<String> {
		public static final String FROM_CLASS_PATH = "From class path";

		@Override
		public String get() {
			return FROM_CLASS_PATH;
		}
	}
}
