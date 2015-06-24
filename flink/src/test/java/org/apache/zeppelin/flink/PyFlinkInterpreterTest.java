package org.apache.zeppelin.flink;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

public class PyFlinkInterpreterTest {

	private static FlinkInterpreter flink;
	private static PyFlinkInterpreter pyflink;
	private static InterpreterContext context;

	@BeforeClass
	public static void setUp() {
		Properties p = new Properties();
		p.setProperty("spark.home", "/Users/till/work/flink/workspace/spark");
		p.setProperty("flink.home", "/Users/till/work/flink/workspace/flink/build-target");
		Properties pflink = new Properties();
		pyflink = new PyFlinkInterpreter(p);
		flink = new FlinkInterpreter(pflink);
		InterpreterGroup group = new InterpreterGroup("flink");
		group.add(flink);
		pyflink.setInterpreterGroup(group);
		pyflink.open();
		context = new InterpreterContext(null, null, null, null, null, null, null);
	}

	@AfterClass
	public static void tearDown() {
		pyflink.close();
		pyflink.destroy();
	}

	@Test
	public void testSimpleStatement() {
		InterpreterResult result = pyflink.interpret("x = 2 * 2", context);
		result = pyflink.interpret("print(x)", context);
		assertEquals("4\n", result.message());
	}
}
