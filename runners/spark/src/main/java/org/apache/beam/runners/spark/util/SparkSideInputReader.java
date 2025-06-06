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
package org.apache.beam.runners.spark.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.translation.SparkPCollectionView;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link SideInputReader} for the SparkRunner. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkSideInputReader implements SideInputReader {
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;

  public SparkSideInputReader(
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs) {
    this.sideInputs = sideInputs;
  }

  @Override
  public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
    // --- validate sideInput.
    checkNotNull(view, "The PCollectionView passed to sideInput cannot be null ");
    KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>> windowedBroadcastHelper =
        sideInputs.get(view.getTagInternal());
    checkNotNull(windowedBroadcastHelper, "SideInput for view " + view + " is not available.");

    // --- sideInput window
    final BoundedWindow sideInputWindow = view.getWindowMappingFn().getSideInputWindow(window);

    // --- match the appropriate sideInput window.
    // a tag will point to all matching sideInputs, that is all windows.
    // now that we've obtained the appropriate sideInputWindow, all that's left is to filter by it.
    final SideInputBroadcast<?> sideInputBroadcast = windowedBroadcastHelper.getValue();
    Iterable<WindowedValue<?>> availableSideInputs =
        (Iterable<WindowedValue<?>>) sideInputBroadcast.getValue();

    final Stream<WindowedValue<?>> stream =
        StreamSupport.stream(availableSideInputs.spliterator(), false)
            .filter(
                sideInputCandidate -> {
                  if (sideInputCandidate == null) {
                    return false;
                  }
                  return Iterables.contains(sideInputCandidate.getWindows(), sideInputWindow);
                });
    final List<?> sideInputForWindow =
        this.getSideInputForWindow(sideInputBroadcast.getSparkPCollectionViewType(), stream);

    switch (view.getViewFn().getMaterialization().getUrn()) {
      case Materializations.ITERABLE_MATERIALIZATION_URN:
        {
          ViewFn<IterableView, T> viewFn = (ViewFn<IterableView, T>) view.getViewFn();
          return viewFn.apply(() -> sideInputForWindow);
        }
      case Materializations.MULTIMAP_MATERIALIZATION_URN:
        {
          ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
          Coder<?> keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
          return viewFn.apply(
              InMemoryMultimapSideInputView.fromIterable(keyCoder, (Iterable) sideInputForWindow));
        }
      default:
        throw new IllegalStateException(
            String.format(
                "Unknown side input materialization format requested '%s'",
                view.getViewFn().getMaterialization().getUrn()));
    }
  }

  /**
   * Extracts side input values from windowed values based on the collection view type.
   *
   * <p>For {@link SparkPCollectionView.Type#STATIC} view types, simply extracts the value from each
   * {@link WindowedValue}.
   *
   * <p>For {@link SparkPCollectionView.Type#STREAMING} view types, performs additional processing
   * by flattening any List values, as streaming side inputs arrive as collections that need to be
   * processed individually.
   *
   * @param sparkPCollectionViewType the type of PCollection view (STATIC or STREAMING)
   * @param stream the stream of WindowedValues filtered for the current window
   * @return a list of extracted side input values
   */
  private List<?> getSideInputForWindow(
      SparkPCollectionView.Type sparkPCollectionViewType, Stream<WindowedValue<?>> stream) {
    switch (sparkPCollectionViewType) {
      case STATIC:
        return stream.map(WindowedValue::getValue).collect(Collectors.toList());
      case STREAMING:
        return stream
            .flatMap(
                (WindowedValue<?> windowedValue) -> {
                  final Object value = windowedValue.getValue();
                  // Streaming side inputs arrive as List collections.
                  // These lists need to be flattened to process each element individually.
                  if (value instanceof List) {
                    final List<?> list = (List) value;
                    return list.stream();
                  } else {
                    return Stream.of(value);
                  }
                })
            .collect(Collectors.toList());
      default:
        throw new IllegalStateException(
            String.format("Unknown pcollection view type %s", sparkPCollectionViewType));
    }
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs != null && sideInputs.isEmpty();
  }
}
