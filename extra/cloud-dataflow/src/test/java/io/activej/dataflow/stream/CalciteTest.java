package io.activej.dataflow.stream;

import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.dataflow.calcite.RecordFunction;
import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.dataflow.calcite.inject.CalciteServerModule;
import io.activej.dataflow.calcite.inject.SerializersModule;
import io.activej.dataflow.calcite.inject.SqlFunctionModule;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumerToList;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.common.Checks.checkState;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.proto.ProtobufUtils.ofObject;
import static io.activej.dataflow.stream.DataflowTest.createCommon;
import static io.activej.dataflow.stream.DataflowTest.getFreeListenAddress;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CalciteTest {

	@ClassRule
	public static final EventloopRule eventloopRule  = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static final String STUDENT_TABLE_NAME = "student";
	public static final String DEPARTMENT_TABLE_NAME = "department";
	public static final String REGISTRY_TABLE_NAME = "registry";

	private static final List<Student> STUDENT_LIST = List.of(
			new Student(1, "John", "Doe", 1),
			new Student(2, "Bob", "Black", 2),
			new Student(3, "John", "Truman", 2));
	private static final List<Department> DEPARTMENT_LIST = List.of(
			new Department(1, "Math"),
			new Department(2, "Language"),
			new Department(3, "History"));
	private static final List<Registry> REGISTRY_LIST = List.of(
			new Registry(1, Map.of("John", 1, "Jack", 2), List.of("google.com", "amazon.com")),
			new Registry(2, Map.of(), List.of("wikipedia.org")),
			new Registry(3, Map.of("Sarah", 53, "Rob", 7564, "John", 18), List.of("yahoo.com", "ebay.com", "netflix.com")));

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private ExecutorService executor;
	private ExecutorService sortingExecutor;
	private SqlDataflow sqlDataflow;
	private DataflowServer server;

	@Before
	public void setUp() throws IOException {
		executor = Executors.newSingleThreadExecutor();
		sortingExecutor = Executors.newSingleThreadExecutor();

		InetSocketAddress address = getFreeListenAddress();

		List<Partition> partitions = List.of(new Partition(address));
		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), partitions)
				.install(new SerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.bind(new Key<List<Partition>>() {}).toInstance(partitions)
				.bind(DataflowSchema.class).to(() -> DataflowSchema.create(
						Map.of(STUDENT_TABLE_NAME, DataflowTable.create(Student.class, new StudentToRecord(), ofObject(StudentToRecord::new)),
								DEPARTMENT_TABLE_NAME, DataflowTable.create(Department.class, new DepartmentToRecord(), ofObject(DepartmentToRecord::new)),
								REGISTRY_TABLE_NAME, DataflowTable.create(Registry.class, new RegistryToRecord(), ofObject(RegistryToRecord::new)))))
				.build();

		Module clientModule = Modules.combine(common,
				new SqlFunctionModule(),
				new CalciteClientModule());
		sqlDataflow = Injector.of(clientModule).getInstance(SqlDataflow.class);


		Module serverModule = ModuleBuilder.create()
				.install(common)
				.install(new CalciteServerModule())
				.bind(datasetId(STUDENT_TABLE_NAME)).toInstance(STUDENT_LIST)
				.bind(datasetId(DEPARTMENT_TABLE_NAME)).toInstance(DEPARTMENT_LIST)
				.bind(datasetId(REGISTRY_TABLE_NAME)).toInstance(REGISTRY_LIST)
				.build();

		server = Injector.of(serverModule).getInstance(DataflowServer.class).withListenAddress(address);
		server.listen();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
		sortingExecutor.shutdownNow();
	}

	@SerializeRecord
	public record Student(int id, String firstName, String lastName, int dept) {
	}

	@SerializeRecord
	public record Department(int id, String departmentName) {
	}

	@SerializeRecord
	public record Registry(int id, Map<String, Integer> counters, List<String> domains) {
	}

	@Test
	public void testSelectAllStudents() {
		List<Record> result = query("SELECT * FROM student");

		assertEquals(STUDENT_LIST,
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testSelectAllDepartments() {
		List<Record> result = query("SELECT * FROM department");

		assertEquals(DEPARTMENT_LIST,
				result.stream()
						.map(CalciteTest::toDepartment)
						.toList());
	}

	@Test
	public void testSelectDepartmentsFields() {
		List<Record> result = query("SELECT departmentName FROM department");

		List<String> expected = DEPARTMENT_LIST.stream()
				.map(department -> department.departmentName)
				.toList();

		assertEquals(expected, result.stream()
				.map(record -> record.get(0))
				.toList());
	}

	@Test
	public void testSelectStudentsFields() {
		List<Record> result = query("SELECT dept, firstName FROM student");

		assertEquals(STUDENT_LIST.size(), result.size());
		for (int i = 0; i < result.size(); i++) {
			Student student = STUDENT_LIST.get(i);
			Record record = result.get(i);

			assertEquals(student.dept, record.getInt(0));
			assertEquals(student.firstName, record.get(1));
		}
	}

	@Test
	public void testSubSelect() {
		List<Record> result = query("""
				SELECT firstName, id
				FROM (SELECT id, firstName, dept from student)
				""");

		assertEquals(STUDENT_LIST.size(), result.size());
		for (int i = 0; i < result.size(); i++) {
			Student student = STUDENT_LIST.get(i);
			Record record = result.get(i);

			assertEquals(student.firstName, record.get(0));
			assertEquals(student.id, record.getInt(1));
		}
	}

	@Test
	public void testWhereEqualTrue() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE 1 = 1
				""");

		assertEquals(STUDENT_LIST,
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereEqualFalse() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE 1 = 2
				""");

		assertTrue(result.isEmpty());
	}

	@Test
	public void testWhereEqualScalar() {
		List<Record> result = query("""
				SELECT firstName, lastName FROM student
				WHERE id = 2
				""");

		assertEquals(1, result.size());
		Record record = result.get(0);

		assertEquals("Bob", record.get(0));
		assertEquals("Black", record.get(1));
	}

	@Test
	public void testWhereEqualField() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id = dept
				""");

		assertEquals(Arrays.asList(STUDENT_LIST.get(0), STUDENT_LIST.get(1)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereNotEqualTrue() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE 1 <> 1
				""");

		assertTrue(result.isEmpty());
	}

	@Test
	public void testWhereNotEqualFalse() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE 1 <> 2
				""");

		assertEquals(STUDENT_LIST,
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereNotEqualScalar() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id <> 2
				""");

		assertEquals(List.of(STUDENT_LIST.get(0), STUDENT_LIST.get(2)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereNotEqualField() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id <> dept
				""");

		assertEquals(List.of(STUDENT_LIST.get(2)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereGreaterThanScalar() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id > 1
				""");

		assertEquals(Arrays.asList(STUDENT_LIST.get(1), STUDENT_LIST.get(2)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereGreaterThanField() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id > dept
				""");

		assertEquals(List.of(STUDENT_LIST.get(2)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereGreaterThanOrEqualScalar() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id >= 2
				""");

		assertEquals(Arrays.asList(STUDENT_LIST.get(1), STUDENT_LIST.get(2)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereGreaterThanOrEqualField() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE dept >= id
				""");

		assertEquals(List.of(STUDENT_LIST.get(0), STUDENT_LIST.get(1)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereLessThanScalar() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id < 2
				""");

		assertEquals(List.of(STUDENT_LIST.get(0)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereLessThanOrEqualScalar() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id <= 2
				""");

		assertEquals(List.of(STUDENT_LIST.get(0), STUDENT_LIST.get(1)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereAndEqual() {
		List<Record> result = query("""
				SELECT id FROM student
				WHERE firstName = 'John' AND dept = 1
				""");

		assertEquals(1, result.size());
		Record record = result.get(0);

		assertEquals(1, record.getInt(0));
	}

	@Test
	public void testWhereOrEqual() {
		List<Record> result = query("""
				SELECT id FROM student
				WHERE firstName = 'Bob' OR dept = 2
				""");

		assertEquals(2, result.size());

		assertEquals(2, result.get(0).getInt(0));
		assertEquals(3, result.get(1).getInt(0));
	}

	@Test
	public void testWhereBetween() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id BETWEEN 1 AND 2
				""");

		assertEquals(List.of(STUDENT_LIST.get(0), STUDENT_LIST.get(1)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereIn() {
		List<Record> result = query("""
				SELECT * FROM student
				WHERE id IN (1, 3, 5)
				""");

		assertEquals(List.of(STUDENT_LIST.get(0), STUDENT_LIST.get(2)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testWhereLikeStartsWithJ() {
		doTestWhereLike("J%", 1, 3);
	}

	@Test
	public void testWhereLikeEndsWithHn() {
		doTestWhereLike("%hn", 1, 3);
	}

	@Test
	public void testWhereStartsWithB() {
		doTestWhereLike("B%", 2);
	}

	@Test
	public void testWhereSecondLetterIsO() {
		doTestWhereLike("_o%", 1, 2, 3);
	}

	@Test
	public void testWhereMatchAll() {
		doTestWhereLike("%", 1, 2, 3);
	}

	@Test
	public void testWhereNoMatch() {
		doTestWhereLike("A");
	}

	private void doTestWhereLike(String firstNamePattern, int... expectedIds) {
		List<Record> result = query("SELECT * FROM student " +
				"WHERE firstName LIKE '" + firstNamePattern + '\'');
		assertEquals(Arrays.stream(expectedIds).mapToObj(id -> STUDENT_LIST.get(id - 1)).toList(),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	@Test
	public void testMapGetQuery() {
		List<Record> result = query("""
				SELECT id, MAP_GET(counters, 'John') FROM registry
				""");

		for (int i = 0; i < REGISTRY_LIST.size(); i++) {
			Registry registry = REGISTRY_LIST.get(i);
			Record record = result.get(i);

			assertEquals(registry.id, record.getInt(0));
			assertEquals(registry.counters.get("John"), record.get(1));
		}
	}

	@Test
	public void testListGetQuery() {
		List<Record> result = query("""
				SELECT id, LIST_GET(domains, 1) FROM registry
				""");

		int index = 1;
		for (int i = 0; i < REGISTRY_LIST.size(); i++) {
			Registry registry = REGISTRY_LIST.get(i);
			Record record = result.get(i);

			assertEquals(registry.id, record.getInt(0));
			List<String> domains = registry.domains;
			assertEquals(index >= domains.size() ? null : domains.get(index), record.get(index));
		}
	}

	@Test
	public void testJoin() {
		List<Record> result = query("""
				SELECT * FROM student
				JOIN department
				ON student.dept = department.id
				""");

		assertEquals(3, result.size());

		Record first = result.get(0);
		Student firstStudent = STUDENT_LIST.get(0);

		assertEquals(first.getInt(0), firstStudent.id);
		assertEquals(first.get(1), firstStudent.firstName);
		assertEquals(first.get(2), firstStudent.lastName);
		assertEquals(first.getInt(3), firstStudent.dept);
		Department firstDepartment = DEPARTMENT_LIST.get(0);
		assertEquals(first.getInt(4), firstDepartment.id);
		assertEquals(first.get(5), firstDepartment.departmentName);

		Record second = result.get(1);
		Student secondStudent = STUDENT_LIST.get(1);
		assertEquals(second.getInt(0), secondStudent.id);
		assertEquals(second.get(1), secondStudent.firstName);
		assertEquals(second.get(2), secondStudent.lastName);
		assertEquals(second.getInt(3), secondStudent.dept);
		Department secondDepartment = DEPARTMENT_LIST.get(1);
		assertEquals(second.getInt(4), secondDepartment.id);
		assertEquals(second.get(5), secondDepartment.departmentName);

		Record third = result.get(2);
		Student thirdStudent = STUDENT_LIST.get(2);
		assertEquals(third.getInt(0), thirdStudent.id);
		assertEquals(third.get(1), thirdStudent.firstName);
		assertEquals(third.get(2), thirdStudent.lastName);
		assertEquals(third.getInt(3), thirdStudent.dept);
		assertEquals(third.getInt(4), secondDepartment.id);
		assertEquals(third.get(5), secondDepartment.departmentName);
	}

	@Test
	public void testJoinByFields() {
		List<Record> result = query("""
				SELECT student.id, department.departmentName FROM student
				JOIN department
				ON student.dept = department.id
				""");

		assertEquals(3, result.size());

		Record first = result.get(0);
		assertEquals(2, first.getScheme().size());
		assertEquals(1, first.getInt(0));
		assertEquals("Math", first.get(1));

		Record second = result.get(1);
		assertEquals(2, second.getInt(0));
		assertEquals("Language", second.get(1));

		Record third = result.get(2);
		assertEquals(3, third.getInt(0));
		assertEquals("Language", third.get(1));
	}

	@Test
	public void testOrderBy() {
		List<Record> result = query("""
				SELECT * FROM student
				ORDER BY firstName ASC, id DESC
				""");

		assertEquals(List.of(STUDENT_LIST.get(1), STUDENT_LIST.get(2), STUDENT_LIST.get(0)),
				result.stream()
						.map(CalciteTest::toStudent)
						.toList());
	}

	private List<Record> query(String sql) {
		StreamConsumerToList<Record> resultConsumer = StreamConsumerToList.create();

		return await(sqlDataflow.query(sql)
				.then(supplier -> supplier.streamTo(resultConsumer))
				.whenComplete(server::close)
				.map($ -> resultConsumer.getList()));
	}

	public static final class StudentToRecord implements RecordFunction<Student> {
		private final RecordScheme scheme = ofJavaRecord(Student.class);

		@Override
		public Record apply(Student student) {
			Record record = scheme.record();
			record.set("id", student.id);
			record.set("firstName", student.firstName);
			record.set("lastName", student.lastName);
			record.set("dept", student.dept);
			return record;
		}
	}

	public static final class DepartmentToRecord implements RecordFunction<Department> {
		private final RecordScheme scheme = ofJavaRecord(Department.class);

		@Override
		public Record apply(Department department) {
			Record record = scheme.record();
			record.set("id", department.id);
			record.set("departmentName", department.departmentName);
			return record;
		}
	}

	public static final class RegistryToRecord implements RecordFunction<Registry> {
		private final RecordScheme scheme = ofJavaRecord(Registry.class);

		@Override
		public Record apply(Registry registry) {
			Record record = scheme.record();
			record.set("id", registry.id);
			record.set("counters", registry.counters);
			record.set("domains", registry.domains);
			return record;
		}
	}

	private static Department toDepartment(Record record) {
		return new Department(record.getInt(0), record.get(1));
	}

	private static Student toStudent(Record record) {
		return new Student(record.getInt(0), record.get(1), record.get(2), record.getInt(3));
	}

	private static RecordScheme ofJavaRecord(Class<?> recordClass) {
		checkState(recordClass.isRecord());

		RecordScheme recordScheme = RecordScheme.create();
		RecordComponent[] recordComponents = recordClass.getRecordComponents();

		for (RecordComponent recordComponent : recordComponents) {
			recordScheme.addField(recordComponent.getName(), recordComponent.getGenericType());
		}

		return recordScheme.build();
	}
}
