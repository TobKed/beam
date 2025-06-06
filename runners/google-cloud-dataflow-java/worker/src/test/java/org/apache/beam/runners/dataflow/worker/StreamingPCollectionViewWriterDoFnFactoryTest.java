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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link StreamingPCollectionViewWriterDoFnFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class StreamingPCollectionViewWriterDoFnFactoryTest {
  @Test
  public void testConstruction() throws Exception {
    DataflowOperationContext mockOperationContext = Mockito.mock(DataflowOperationContext.class);
    DataflowExecutionContext mockExecutionContext = Mockito.mock(DataflowExecutionContext.class);
    DataflowStepContext mockStepContext =
        Mockito.mock(StreamingModeExecutionContext.StepContext.class);
    when(mockExecutionContext.getStepContext(mockOperationContext)).thenReturn(mockStepContext);

    CloudObject coder =
        CloudObjects.asCloudObject(
            WindowedValues.getFullCoder(BigEndianIntegerCoder.of(), GlobalWindow.Coder.INSTANCE),
            /*sdkComponents=*/ null);
    ParDoFn parDoFn =
        new StreamingPCollectionViewWriterDoFnFactory()
            .create(
                null /* pipeline options */,
                CloudObject.fromSpec(
                    ImmutableMap.of(
                        PropertyNames.OBJECT_TYPE_NAME,
                        "StreamingPCollectionViewWriterDoFn",
                        PropertyNames.ENCODING,
                        coder,
                        WorkerPropertyNames.SIDE_INPUT_ID,
                        "test-side-input-id")),
                null /* side input infos */,
                null /* main output tag */,
                null /* output tag to receiver index */,
                mockExecutionContext,
                mockOperationContext);
    assertThat(parDoFn, instanceOf(StreamingPCollectionViewWriterParDoFn.class));
  }
}
