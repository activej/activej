package io.activej.ot.repository;

import io.activej.common.exception.MalformedDataException;
import io.activej.ot.utils.TestAdd;
import io.activej.ot.utils.TestOp;
import io.activej.ot.utils.TestSet;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static io.activej.json.JsonUtils.fromJson;
import static io.activej.json.JsonUtils.toJson;
import static io.activej.ot.utils.Utils.TEST_OP_CODEC;
import static org.junit.Assert.assertEquals;

@Ignore
public class TestOpJsonTest {
	@Test
	public void testJson() throws IOException, MalformedDataException {
		{
			TestAdd testAdd = new TestAdd(1);
			String json = toJson(TEST_OP_CODEC, testAdd);
			TestOp testAdd2 = fromJson(TEST_OP_CODEC, json);
			assertEquals(testAdd, testAdd2);
		}

		{
			TestSet testSet = new TestSet(0, 4);
			String json = toJson(TEST_OP_CODEC, testSet);
			TestOp testSet2 = fromJson(TEST_OP_CODEC, json);
			assertEquals(testSet, testSet2);
		}
	}
}
