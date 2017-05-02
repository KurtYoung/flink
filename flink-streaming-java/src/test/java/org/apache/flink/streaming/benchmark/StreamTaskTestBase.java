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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Assert;

public abstract class StreamTaskTestBase<T> extends TestLogger {

	protected StreamMockEnvironment mockEnv;

	protected AbstractInvokable task;

	protected Configuration jobConfig;
	protected Configuration taskConfig;
	protected ExecutionConfig executionConfig;
	protected StreamConfig streamConfig;

	protected MockInputSplitProvider inputSplitProvider;

	protected long memorySize = 0;
	protected int bufferSize = 0;

	public StreamTaskTestBase() {
		this.jobConfig = new Configuration();
		this.taskConfig = new Configuration();
		this.executionConfig = new ExecutionConfig();
		this.streamConfig = new StreamConfig(taskConfig);
	}

	public void setTask(AbstractInvokable task) {
		this.task = task;
		this.task.setEnvironment(mockEnv);
	}

	public void initEnvironment(long memorySize, int bufferSize) {
		this.memorySize = memorySize;
		this.bufferSize = bufferSize;
		this.inputSplitProvider = new MockInputSplitProvider();
		this.mockEnv = new StreamMockEnvironment(jobConfig, taskConfig, executionConfig,
				memorySize, inputSplitProvider, bufferSize);
	}

	public void setInputGates(InputGate[] inputGates,
			TypeSerializer<T> typeSerializer) {
		for (InputGate inputGate : inputGates) {
			mockEnv.addInputGate(inputGate);
		}

		List<StreamEdge> inPhysicalEdges = new LinkedList<StreamEdge>();

		StreamOperator<T> dummyOperator = new AbstractStreamOperator<T>() {
			private static final long serialVersionUID = 1L;
		};

		StreamNode sourceVertexDummy = new StreamNode(null, 0, "default group", dummyOperator, "source dummy", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(null, 1, "default group", dummyOperator, "target dummy", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);

		for (int i = 0; i < inputGates.length; i++) {
			StreamEdge streamEdge = new StreamEdge(sourceVertexDummy,
					targetVertexDummy,
					i + 1,
					new LinkedList<String>(),
					new BroadcastPartitioner<>(),
					null /* output tag */);
			inPhysicalEdges.add(streamEdge);
		}

		streamConfig.setInPhysicalEdges(inPhysicalEdges);
		streamConfig.setNumberOfInputs(inputGates.length);
		streamConfig.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);

		streamConfig.setTypeSerializerIn1(typeSerializer);
		streamConfig.setTypeSerializerIn2(typeSerializer);
	}

	public void setOutputWriter(ResultPartitionWriter writer, TypeSerializer<T> typeSerializer) {
		mockEnv.addOutputWriter(writer);
		streamConfig.setNumberOfOutputs(1);

		streamConfig.setTypeSerializerOut(typeSerializer);
		streamConfig.setTimeCharacteristic(TimeCharacteristic.EventTime);
		streamConfig.setOutputSelectors(Collections.<OutputSelector<?>>emptyList());
		streamConfig.setVertexID(0);

		StreamOperator<T> dummyOperator = new AbstractStreamOperator<T>() {
			private static final long serialVersionUID = 1L;
		};

		List<StreamEdge> outEdgesInOrder = new LinkedList<StreamEdge>();
		StreamNode sourceVertexDummy = new StreamNode(null, 0, "group", dummyOperator, "source dummy", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(null, 1, "group", dummyOperator, "target dummy", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);

		outEdgesInOrder.add(new StreamEdge(sourceVertexDummy, targetVertexDummy, 0, new LinkedList<String>(), new BroadcastPartitioner<Object>(), null /* output tag */));

		streamConfig.setOutEdgesInOrder(outEdgesInOrder);
		streamConfig.setNonChainedOutputs(outEdgesInOrder);
	}

	public MemoryManager getMemoryManager() {
		return this.mockEnv.getMemoryManager();
	}

	@After
	public void shutdownIOManager() throws Exception {
		this.mockEnv.getIOManager().shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", this.mockEnv.getIOManager().isProperlyShutDown());
	}

	@After
	public void shutdownMemoryManager() throws Exception {
		if (this.memorySize > 0) {
			MemoryManager memMan = getMemoryManager();
			memMan.releaseAll(task);
			if (memMan != null) {
				Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
				memMan.shutdown();
			}
		}
	}
}