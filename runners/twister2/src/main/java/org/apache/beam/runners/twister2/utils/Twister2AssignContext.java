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
package org.apache.beam.runners.twister2.utils;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

/** doc. */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
public class Twister2AssignContext<T, W extends BoundedWindow>
    extends WindowFn<T, W>.AssignContext {

  private final WindowedValue<T> value;

  public Twister2AssignContext(WindowFn<T, W> fn, WindowedValue<T> value) {
    fn.super();
    this.value = value;
  }

  @Override
  public T element() {
    return value.getValue();
  }

  @Override
  public Instant timestamp() {
    return value.getTimestamp();
  }

  @Override
  public BoundedWindow window() {
    return Iterables.getOnlyElement(value.getWindows());
  }
}
