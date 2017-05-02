/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.util.NoOpFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RuntimeComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.DummyTwoInputDriver;
import org.apache.flink.runtime.operators.DummyTwoInputStreamOperatorDriver;
import org.apache.flink.runtime.operators.JoinDriver;
import org.apache.flink.runtime.operators.MapDriver;
import org.apache.flink.runtime.operators.NoOpDriver;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Ignore;
import org.junit.Test;

public class BatchTaskBenchmark extends BatchTaskTestBase {

	public static final TupleTypeInfo<Tuple2<Integer, String>> tupleTypeInfo =
			new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

	private static final int MEMORY_MANAGER_SIZE = 1024 * 1024 * 3;

	private static final int NETWORK_BUFFER_SIZE = 1024 * 32;

	private static long memorySize = 104857600;

	private static int size = 16;

	private static int range = 100000;

	private static int count = 5;

	private static void getParameters() {
		memorySize = Long.valueOf(System.getProperty("memory", "104857600")); // 100M
		size = Integer.valueOf(System.getProperty("size", "16"));
		range = Integer.valueOf(System.getProperty("range", "100000"));
		count = Integer.valueOf(System.getProperty("count", "1000000"));
		System.out.println("run with parameters:\n"
				+ "record size: " + size  + "\n"
				+ "memory size: " + memorySize / 1024 / 1024 + " mb\n"
				+ "key range: " + range + "\n");
	}

	@Ignore
	@Test
	public void testEmpty() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<NoOpFunction, Tuple2<Integer, String>> testTask = new BatchTask<>();
		registerTask(testTask, NoOpDriver.class, NoOpFunction.class);

		MutableObjectIterator<Tuple2<Integer, String>> input = new RandomTupleGenerator(size, count, range, false);
		TestingInputGate<Tuple2<Integer, String>> inputGate = new TestingInputGate<>(
				input, tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig()));
		addInputGate(inputGate, 0, new IntStringTupleSerializerFactory(tupleTypeInfo, mockEnv.getExecutionConfig()));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.UNARY_NO_OP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new IntStringTupleSerializerFactory(tupleTypeInfo, mockEnv.getExecutionConfig()));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("write count: %d\nprocessed count: %d\naverage: %.2f\n",
				writer.getCount().get(), inputGate.getProcessedCount(), inputGate.getProcessedCount() / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testMap() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMap, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, MapDriver.class, DummyMap.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());
		MutableObjectIterator<StreamElement> input = new RandomStreamElementGenerator<>(
				new RandomTupleGenerator(size, count, range, false),
				tupleTypeSerializer);

		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);
		TestingInputGate<StreamElement> inputGate = new TestingInputGate<>(input, streamElementSerializer);
		addInputGate(inputGate, 0, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap count: %d\nwrite count: %d\naverage: %.2f\n",
				inputGate.getProcessedCount(), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testMapFixed() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMap, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, MapDriver.class, DummyMap.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		int stringSize = size - 4;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < stringSize; ++i) {
			sb.append('a');
		}
		Tuple2<Integer, String> fixedRecord = new Tuple2<>(1, sb.toString());
		StreamElement fixedElement = new StreamRecord<>(fixedRecord);
		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);

		FixedInputGate<StreamElement> inputGate = new FixedInputGate<>(count, fixedElement, streamElementSerializer);
		addInputGate(inputGate, 0, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap count: %d\nwrite count: %d\naverage: %.2f\n",
				inputGate.getProcessedCount(), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	public static class DummyMap extends RichMapFunction<StreamElement, StreamElement> {

		private int mapCount = 0;

		@Override
		public StreamElement map(StreamElement value) throws Exception {
			mapCount++;

			return value;
		}
	}

	public static class DummyMapTuple extends RichMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

		private int mapCount = 0;

		@Override
		public Tuple2<Integer, String> map(Tuple2<Integer, String> value) throws Exception {
			mapCount++;

			return value;
		}
	}

	@Ignore
	@Test
	public void testTwoMapTuple() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMapTuple, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, DummyTwoInputDriver.class, DummyMapTuple.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

//		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);

		MutableObjectIterator<Tuple2<Integer, String>> input1 = new RandomTupleGenerator(size, count, range, false);
		MutableObjectIterator<Tuple2<Integer, String>> input2 = new RandomTupleGenerator(size, count, range, false);

		TestingInputGate<Tuple2<Integer, String>> inputGate1 = new TestingInputGate<>(input1, tupleTypeSerializer);
		TestingInputGate<Tuple2<Integer, String>> inputGate2 = new TestingInputGate<>(input2, tupleTypeSerializer);

		addInputGate(inputGate1, 0, new RuntimeSerializerFactory<>(tupleTypeSerializer, tupleTypeInfo.getTypeClass()));
		addInputGate(inputGate2, 1, new RuntimeSerializerFactory<>(tupleTypeSerializer, tupleTypeInfo.getTypeClass()));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.DUMMY_TWO_MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(tupleTypeSerializer, tupleTypeInfo.getTypeClass()));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap1 count: %d\nwrite count: %d\naverage: %.2f\n",
				(inputGate1.getProcessedCount() + inputGate2.getProcessedCount()), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testTwoMapTupleFixed() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMapTuple, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, DummyTwoInputDriver.class, DummyMapTuple.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		int stringSize = size - 4;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < stringSize; ++i) {
			sb.append('a');
		}
		Tuple2<Integer, String> fixedRecord = new Tuple2<>(1, sb.toString());

		FixedInputGate<Tuple2<Integer, String>> inputGate1 = new FixedInputGate<>(count, fixedRecord, tupleTypeSerializer);
		FixedInputGate<Tuple2<Integer, String>> inputGate2 = new FixedInputGate<>(count, fixedRecord, tupleTypeSerializer);

		addInputGate(inputGate1, 0, new RuntimeSerializerFactory<>(tupleTypeSerializer, tupleTypeInfo.getTypeClass()));
		addInputGate(inputGate2, 1, new RuntimeSerializerFactory<>(tupleTypeSerializer, tupleTypeInfo.getTypeClass()));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.DUMMY_TWO_MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(tupleTypeSerializer, tupleTypeInfo.getTypeClass()));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap1 count: %d\nwrite count: %d\naverage: %.2f\n",
				(inputGate1.getProcessedCount() + inputGate2.getProcessedCount()), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testTwoMap() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMap, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, DummyTwoInputDriver.class, DummyMap.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);

		MutableObjectIterator<StreamElement> input1 = new RandomStreamElementGenerator<>(
				new RandomTupleGenerator(size, count, range, false),
				tupleTypeSerializer);
		MutableObjectIterator<StreamElement> input2 = new RandomStreamElementGenerator<>(
				new RandomTupleGenerator(size, count, range, false),
				tupleTypeSerializer);

		TestingInputGate<StreamElement> inputGate1 = new TestingInputGate<>(input1, streamElementSerializer);
		TestingInputGate<StreamElement> inputGate2 = new TestingInputGate<>(input2, streamElementSerializer);

		addInputGate(inputGate1, 0, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));
		addInputGate(inputGate2, 1, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.DUMMY_TWO_MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap1 count: %d\nwrite count: %d\naverage: %.2f\n",
				(inputGate1.getProcessedCount() + inputGate2.getProcessedCount()), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testTwoMapFixed() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMap, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, DummyTwoInputDriver.class, DummyMap.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		int stringSize = size - 4;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < stringSize; ++i) {
			sb.append('a');
		}
		Tuple2<Integer, String> fixedRecord = new Tuple2<>(1, sb.toString());
		StreamElement fixedElement = new StreamRecord<>(fixedRecord);
		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);

		FixedInputGate<StreamElement> inputGate1 = new FixedInputGate<>(count, fixedElement, streamElementSerializer);
		FixedInputGate<StreamElement> inputGate2 = new FixedInputGate<>(count, fixedElement, streamElementSerializer);

		addInputGate(inputGate1, 0, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));
		addInputGate(inputGate2, 1, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.DUMMY_TWO_MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap1 count: %d\nwrite count: %d\naverage: %.2f\n",
				(inputGate1.getProcessedCount() + inputGate2.getProcessedCount()), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testTwoStreamMap() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMap, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, DummyTwoInputStreamOperatorDriver.class, DummyMap.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);

		MutableObjectIterator<StreamElement> input1 = new RandomStreamElementGenerator<>(
				new RandomTupleGenerator(size, count, range, false),
				tupleTypeSerializer);
		MutableObjectIterator<StreamElement> input2 = new RandomStreamElementGenerator<>(
				new RandomTupleGenerator(size, count, range, false),
				tupleTypeSerializer);

		TestingInputGate<StreamElement> inputGate1 = new TestingInputGate<>(input1, streamElementSerializer);
		TestingInputGate<StreamElement> inputGate2 = new TestingInputGate<>(input2, streamElementSerializer);

		addInputGate(inputGate1, 0, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));
		addInputGate(inputGate2, 1, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.DUMMY_TWO_STREAM_MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap1 count: %d\nwrite count: %d\naverage: %.2f\n",
				(inputGate1.getProcessedCount() + inputGate2.getProcessedCount()), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testTwoStreamMapFixed() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<DummyMap, StreamElement> testTask = new BatchTask<>();
		registerTask(testTask, DummyTwoInputStreamOperatorDriver.class, DummyMap.class);

		TypeSerializer<Tuple2<Integer, String>> tupleTypeSerializer =
				tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig());

		int stringSize = size - 4;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < stringSize; ++i) {
			sb.append('a');
		}
		Tuple2<Integer, String> fixedRecord = new Tuple2<>(1, sb.toString());
		StreamElement fixedElement = new StreamRecord<>(fixedRecord);
		TypeSerializer<StreamElement> streamElementSerializer = new StreamElementSerializer<>(tupleTypeSerializer);

		FixedInputGate<StreamElement> inputGate1 = new FixedInputGate<>(count, fixedElement, streamElementSerializer);
		FixedInputGate<StreamElement> inputGate2 = new FixedInputGate<>(count, fixedElement, streamElementSerializer);

		addInputGate(inputGate1, 0, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));
		addInputGate(inputGate2, 1, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.DUMMY_TWO_STREAM_MAP);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new RuntimeSerializerFactory<>(streamElementSerializer, StreamElement.class));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(String.format("input count: %d\nmap1 count: %d\nwrite count: %d\naverage: %.2f\n",
				(inputGate1.getProcessedCount() + inputGate2.getProcessedCount()), testTask.getStub().mapCount,
				writer.getCount().get(), testTask.getStub().mapCount / sec));

		testTask.cancel();
	}

	@Ignore
	@Test
	public void testJoin() throws Exception {
		initEnvironment(memorySize, NETWORK_BUFFER_SIZE);
		mockEnv.getExecutionConfig().enableObjectReuse();

		BatchTask<MockJoinStub, Tuple2<Integer, String>> testTask = new BatchTask<>();
		registerTask(testTask, JoinDriver.class, MockJoinStub.class);

		MutableObjectIterator<Tuple2<Integer, String>> input1 = new RandomTupleGenerator(size, range, range, true);
		MutableObjectIterator<Tuple2<Integer, String>> input2 = new RandomTupleGenerator(size, count, range, false);
		TestingInputGate<Tuple2<Integer, String>> inputGate1 = new TestingInputGate<>(
				input1, tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig()));
		TestingInputGate<Tuple2<Integer, String>> inputGate2 = new TestingInputGate<>(
				input2, tupleTypeInfo.createSerializer(mockEnv.getExecutionConfig()));
		addInputGate(inputGate1, 0, new IntStringTupleSerializerFactory(tupleTypeInfo, mockEnv.getExecutionConfig()));
		addInputGate(inputGate2, 1, new IntStringTupleSerializerFactory(tupleTypeInfo, mockEnv.getExecutionConfig()));

		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());

		RuntimeComparatorFactory compFact1 = new RuntimeComparatorFactory(
				new TupleComparator<>(
						new int[]{0},
						new TypeComparator<?>[]{new IntComparator(true)},
						new TypeSerializer<?>[]{IntSerializer.INSTANCE}
		));
		RuntimeComparatorFactory compFact2 = new RuntimeComparatorFactory(
				new TupleComparator<>(
						new int[]{0},
						new TypeComparator<?>[]{new IntComparator(true)},
						new TypeSerializer<?>[]{IntSerializer.INSTANCE}
				));
		conf.setDriverComparator(compFact1, 0);
		conf.setDriverComparator(compFact2, 1);
		conf.setDriverPairComparator(new RuntimePairComparatorFactory());

		conf.setRelativeMemoryDriver(1.0);
		conf.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);

		TestingResultPartitionWriter writer = new TestingResultPartitionWriter();
		addOutputWriter(writer, new IntStringTupleSerializerFactory(tupleTypeInfo, mockEnv.getExecutionConfig()));

		long start = System.nanoTime();
		testTask.invoke();
		long end = System.nanoTime();
		double sec = (end - start) / 1_000_000_000.0;

		System.out.println(
				String.format("build count: %d\nprobe count: %d\njoined count: %d\naverage: %.2f\n",
						inputGate1.getProcessedCount(), inputGate2.getProcessedCount(),
						testTask.getStub().getJoinedCount(),
						testTask.getStub().getJoinedCount() / sec));

		testTask.cancel();
	}

	public static class MockJoinStub extends RichFlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		private int joinedCount;

		@Override
		public void join(Tuple2<Integer, String> first, Tuple2<Integer, String> second, Collector<Tuple2<Integer, String>> out) throws Exception {
			joinedCount++;
//			if (joinedCount % 100 == 0) {
//				System.out.println("joined, " +
//						"left: (" + first.getField(0, IntValue.class) + ", " + first.getField(1, StringValue.class) +
//						"), right: (" + second.getField(0, IntValue.class) + ", " + second.getField(1, StringValue.class) + ")");
//			}
			out.collect(second);
		}

		public int getJoinedCount() {
			return joinedCount;
		}
	}

	public static class IntStringTupleSerializerFactory implements TypeSerializerFactory<Tuple2<Integer, String>> {

		private TupleTypeInfo<Tuple2<Integer, String>> tupleTypeInfo;
		private final ExecutionConfig executionConfig;

		public IntStringTupleSerializerFactory() {
			this(new TupleTypeInfo<Tuple2<Integer, String>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
					new ExecutionConfig());
		}

		public IntStringTupleSerializerFactory(
				TupleTypeInfo<Tuple2<Integer, String>> tupleTypeInfo,
				ExecutionConfig config) {
			this.tupleTypeInfo = tupleTypeInfo;
			this.executionConfig = config;
		}

		@Override
		public void writeParametersToConfig(Configuration config) {
		}

		@Override
		public void readParametersFromConfig(Configuration config,
				ClassLoader cl) throws ClassNotFoundException {
		}

		@Override
		public TypeSerializer<Tuple2<Integer, String>> getSerializer() {
			return this.tupleTypeInfo.createSerializer(this.executionConfig);
		}

		@Override
		public Class<Tuple2<Integer, String>> getDataType() {
			return null;
		}
	}

	public static void main(String[] args) throws Exception {
		BatchTaskBenchmark benchmark = new BatchTaskBenchmark();

		String test = System.getProperty("test");
		if (test == null) {
			System.err.println("You should specify a test run via -Dtest=...");
			System.exit(1);
		}

		getParameters();

		boolean warmup = Boolean.valueOf(System.getProperty("warmup", "false"));
		if (warmup) {
			System.out.println("============= Warm up JIT ==================");
			for (int i = 0; i < 10; ++i) {
				run(benchmark, test);
				benchmark.shutdownIOManager();
				benchmark.shutdownMemoryManager();
			}
		}

//		System.err.println("Prepare to start test!!!");
//		Thread.sleep(30000);

		System.out.println("============= Test ==================");
		run(benchmark, test);
		benchmark.shutdownIOManager();
		benchmark.shutdownMemoryManager();
	}

	private static void run(BatchTaskBenchmark benchmark, String testName) throws Exception {
		if (testName.equalsIgnoreCase("testEmpty")) {
			benchmark.testEmpty();
		} else if (testName.equalsIgnoreCase("testMap")) {
			benchmark.testMap();
		} else if (testName.equalsIgnoreCase("testMapFixed")) {
			benchmark.testMapFixed();
		} else if (testName.equalsIgnoreCase("testTwoMap")) {
			benchmark.testTwoMap();
		} else if (testName.equalsIgnoreCase("testTwoMapFixed")) {
			benchmark.testTwoMapFixed();
		} else if (testName.equalsIgnoreCase("testTwoMapTuple")) {
			benchmark.testTwoMapTuple();
		} else if (testName.equalsIgnoreCase("testTwoMapTupleFixed")) {
			benchmark.testTwoMapTupleFixed();
		} else if (testName.equalsIgnoreCase("testTwoStreamMap")) {
			benchmark.testTwoStreamMap();
		} else if (testName.equalsIgnoreCase("testTwoStreamMapFixed")) {
			benchmark.testTwoStreamMapFixed();
		} else if (testName.equalsIgnoreCase("testJoin")) {
			benchmark.testJoin();
		} else {
			System.err.println("Unknown test name:" + testName);
			System.exit(1);
		}
	}
}
