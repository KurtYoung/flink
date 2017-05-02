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

package org.apache.flink.streaming.runtime.io;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards watermarks to event subscribers once the
 * {@link StatusWatermarkValve} determines the watermarks from all inputs has advanced, or changes
 * the task's {@link StreamStatus} once status change is toggled.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link TwoInputStreamOperator} concurrently with the timer callback or other things.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public class StreamTwoInputProcessor<IN1, IN2> {

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>>[] currentRecordDeserializers = new RecordDeserializer[2];

	private final DeserializationDelegate<StreamElement>[] deserializationDelegates = new DeserializationDelegate[2];

	private final CheckpointBarrierHandler barrierHandler;

	private final Object lock;

	// ---------------- Status and Watermark Valves ------------------

	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private StreamStatus firstStatus;
	private StreamStatus secondStatus;

	/**
	 * Valves that control how watermarks and stream statuses from the 2 inputs are forwarded.
	 */
	private StatusWatermarkValve[] statusWatermarkValves = new StatusWatermarkValve[2];

	/** Number of input channels the valves need to handle. */
	private final int numInputChannels1;
	private final int numInputChannels2;

	private boolean[] bufferConsumed = new boolean[2];

	private int[] currentChannel = new int[2];

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final TwoInputStreamOperator<IN1, IN2, ?> streamOperator;

	// ---------------- Metrics ------------------

	private long lastEmittedWatermark1;
	private long lastEmittedWatermark2;

	private boolean[] isFinished = new boolean[2];

	private InputSelection inputSelection;

	@SuppressWarnings("unchecked")
	public StreamTwoInputProcessor(
			Collection<InputGate> inputGates1,
			Collection<InputGate> inputGates2,
			TypeSerializer<IN1> inputSerializer1,
			TypeSerializer<IN2> inputSerializer2,
			StatefulTask checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator) throws IOException {


		// determine which unioned channels belong to input 1 and which belong to input 2
		int numInputChannels1 = 0;
		for (InputGate gate: inputGates1) {
			numInputChannels1 += gate.getNumberOfInputChannels();
		}

		this.numInputChannels1 = numInputChannels1;

		final InputGate inputGate = InputGateUtil.createInputGate(inputGates1, inputGates2);

		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
			if (!(maxAlign == -1 || maxAlign > 0)) {
				throw new IllegalConfigurationException(
						TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
								+ " must be positive or -1 (infinite)");
			}
			this.barrierHandler = new BarrierBuffer(inputGate, ioManager, maxAlign);
		}
		else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			this.barrierHandler = new BarrierTracker(inputGate);
		}
		else if (checkpointMode == CheckpointingMode.BATCH) {
			this.barrierHandler = new BatchBarrierHandler(InputGateUtil.createInputGate(inputGates1.toArray(new InputGate[inputGates1.size()])),
					InputGateUtil.createInputGate(inputGates2.toArray(new InputGate[inputGates2.size()])));
		}
		else {
			throw new IllegalArgumentException("Unrecognized CheckpointingMode: " + checkpointMode);
		}

		if (checkpointedTask != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}

		this.numInputChannels2 = inputGate.getNumberOfInputChannels() - numInputChannels1;

		this.lock = checkNotNull(lock);

		StreamElementSerializer<IN1> ser1 = new StreamElementSerializer<>(inputSerializer1);
		this.deserializationDelegates[0] = new NonReusingDeserializationDelegate<>(ser1);

		StreamElementSerializer<IN2> ser2 = new StreamElementSerializer<>(inputSerializer2);
		this.deserializationDelegates[1] = new NonReusingDeserializationDelegate<>(ser2);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
		}

		this.lastEmittedWatermark1 = Long.MIN_VALUE;
		this.lastEmittedWatermark2 = Long.MIN_VALUE;

		this.firstStatus = StreamStatus.ACTIVE;
		this.secondStatus = StreamStatus.ACTIVE;

		currentRecordDeserializers[0] = null;
		currentRecordDeserializers[1] = null;

		bufferConsumed[0] = true;
		bufferConsumed[1] = true;

		isFinished[0] = false;
		isFinished[1] = false;

		this.inputSelection = streamOperator.firstInputSelection();

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValves[0] = new StatusWatermarkValve(numInputChannels1, new ForwardingValveOutputHandler1(streamOperator, lock));
		this.statusWatermarkValves[1] = new StatusWatermarkValve(numInputChannels2, new ForwardingValveOutputHandler2(streamOperator, lock));

	}

	private int getInputIndex(InputSelection inputSelection) {
		if (inputSelection.getSelection() >= 0) {
			return inputSelection.getSelection();
		}
		else {
			if (bufferConsumed[1]) {
				return 0;
			} else {
				return 1;
			}
		}
	}

	public boolean processInput() throws Exception {
		if (isFinished[0] && isFinished[1]) {
			return false;
		}

		while (true) {
			int inputIndex = getInputIndex(inputSelection);
			if (!bufferConsumed[inputIndex]) {
				DeserializationResult result;
				result = currentRecordDeserializers[inputIndex].getNextRecord(deserializationDelegates[inputIndex]);

				if (result.isBufferConsumed()) {
					currentRecordDeserializers[inputIndex].getCurrentBuffer().recycle();
					bufferConsumed[inputIndex] = true;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrWatermark = deserializationDelegates[inputIndex].getInstance();
					if (recordOrWatermark.isWatermark()) {
						statusWatermarkValves[inputIndex].inputWatermark(recordOrWatermark.asWatermark(), currentChannel[inputIndex]);
						continue;
					}
					else if (recordOrWatermark.isStreamStatus()) {
						statusWatermarkValves[inputIndex].inputStreamStatus(recordOrWatermark.asStreamStatus(), currentChannel[inputIndex]);
						continue;
					}
					else if (recordOrWatermark.isLatencyMarker()) {
						synchronized (lock) {
							if (inputIndex == 0) {
								streamOperator.processLatencyMarker1(recordOrWatermark.asLatencyMarker());
							} else {
								streamOperator.processLatencyMarker2(recordOrWatermark.asLatencyMarker());
							}
						}
						continue;
					}
					else {
						if (inputIndex == 0) {
							StreamRecord<IN1> record = recordOrWatermark.asRecord();
							synchronized (lock) {
								streamOperator.setKeyContextElement1(record);
								streamOperator.processElement1(record);
							}
						} else {
							StreamRecord<IN2> record = recordOrWatermark.asRecord();
							synchronized (lock) {
								streamOperator.setKeyContextElement2(record);
								streamOperator.processElement2(record);
							}
						}
						return true;
						}
					}
				}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked(inputSelection);
			if (bufferOrEvent != null) {

				if (bufferOrEvent.isBuffer()) {
					int channelIndex = inputSelection.equals(InputSelection.SECOND) ? (bufferOrEvent.getChannelIndex() + numInputChannels1) : bufferOrEvent.getChannelIndex();
					int channelInputIndex = channelIndex < numInputChannels1 ? 0 : 1;
					currentChannel[channelInputIndex] = channelIndex < numInputChannels1 ? channelIndex : channelIndex - numInputChannels1;
					recordDeserializers[channelIndex].setNextBuffer(bufferOrEvent.getBuffer());
					currentRecordDeserializers[channelInputIndex] = recordDeserializers[channelIndex];
					bufferConsumed[channelInputIndex] = false;

				} else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				if (InputSelection.RANDOM.equals(inputSelection)) {
					streamOperator.endInput1();
					streamOperator.endInput2();
					isFinished[0] = true;
					isFinished[1] = true;
					return false;
				} else if (InputSelection.FIRST.equals(inputSelection)) {
					inputSelection = streamOperator.endInput1();
					isFinished[0] = true;
				} else if (InputSelection.SECOND.equals(inputSelection)) {
					inputSelection = streamOperator.endInput2();
					isFinished[1] = true;
				}
				// how to end with isFinished
				if (isFinished[0] && isFinished[1]) {
					return false;
				}
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
			}
		}
	}

	/**
	 * Sets the metric group for this StreamTwoInputProcessor.
	 *
	 * @param metrics metric group
	 */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		metrics.gauge("currentLowWatermark", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return Math.min(lastEmittedWatermark1, lastEmittedWatermark2);
			}
		});

		metrics.gauge("checkpointAlignmentTime", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return barrierHandler.getAlignmentDurationNanos();
			}
		});
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}

	private class ForwardingValveOutputHandler1 implements StatusWatermarkValve.ValveOutputHandler {
		private final TwoInputStreamOperator<IN1, IN2, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler1(final TwoInputStreamOperator<IN1, IN2, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					lastEmittedWatermark1 = watermark.getTimestamp();
					operator.processWatermark1(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					firstStatus = streamStatus;

					// check if we need to toggle the task's stream status
					if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
						if (streamStatus.isActive()) {
							// we're no longer idle if at least one input has become active
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
						} else if (secondStatus.isIdle()) {
							// we're idle once both inputs are idle
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}

	private class ForwardingValveOutputHandler2 implements StatusWatermarkValve.ValveOutputHandler {
		private final TwoInputStreamOperator<IN1, IN2, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler2(final TwoInputStreamOperator<IN1, IN2, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					lastEmittedWatermark2 = watermark.getTimestamp();
					operator.processWatermark2(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					secondStatus = streamStatus;

					// check if we need to toggle the task's stream status
					if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
						if (streamStatus.isActive()) {
							// we're no longer idle if at least one input has become active
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
						} else if (firstStatus.isIdle()) {
							// we're idle once both inputs are idle
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}
}
