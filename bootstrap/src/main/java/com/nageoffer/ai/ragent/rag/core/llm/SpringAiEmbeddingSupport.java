/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.rag.core.llm;

import org.springframework.ai.embedding.EmbeddingModel;

import java.util.ArrayList;
import java.util.List;

public final class SpringAiEmbeddingSupport {

    private SpringAiEmbeddingSupport() {
    }

    public static List<Float> embedAsList(EmbeddingModel embeddingModel, String text) {
        return toList(embeddingModel.embed(text == null ? "" : text));
    }

    public static List<List<Float>> embedBatchAsList(EmbeddingModel embeddingModel, List<String> texts) {
        if (texts == null || texts.isEmpty()) {
            return List.of();
        }
        List<float[]> vectors = embeddingModel.embed(texts);
        List<List<Float>> result = new ArrayList<>(vectors.size());
        for (float[] vector : vectors) {
            result.add(toList(vector));
        }
        return result;
    }

    public static List<Float> toList(float[] vector) {
        if (vector == null || vector.length == 0) {
            return List.of();
        }
        List<Float> result = new ArrayList<>(vector.length);
        for (float value : vector) {
            result.add(value);
        }
        return result;
    }
}
