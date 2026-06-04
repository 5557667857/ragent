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

package com.nageoffer.ai.ragent.core.chunk;

import com.nageoffer.ai.ragent.framework.exception.ClientException;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 分块嵌入服务
 * 职责单一：为分块结果对象调用嵌入 API 生成向量
 */
@Service
@RequiredArgsConstructor
public class ChunkEmbeddingService {

    private final EmbeddingModel embeddingModel;

    /**
     * 为分块结果对象计算嵌入向量
     * @param chunks         分块结果对象
     */
    public void embed(List<VectorChunk> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            return;
        }
        //检查嵌入向量是否已经计算，避免重复计算
        if (chunks.stream().allMatch(c -> c.getEmbedding() != null && c.getEmbedding().length > 0)) {
            return;
        }
        //提取文本内容列表
        List<String> texts = chunks.stream()
                .map(c -> c.getContent() == null ? "" : c.getContent())
                .collect(Collectors.toList());
        List<float[]> vectors = embeddingModel.embed(texts);
        applyEmbeddings(chunks, vectors);
    }

    private void applyEmbeddings(List<VectorChunk> chunks, List<float[]> vectors) {
        if (vectors == null || vectors.size() != chunks.size()) {
            throw new ClientException("Embedding result size mismatch");
        }
        for (int i = 0; i < chunks.size(); i++) {
            float[] row = vectors.get(i);
            if (row == null || row.length == 0) {
                throw new ClientException("Embedding result missing, index: " + i);
            }
            chunks.get(i).setEmbedding(row);
        }
    }
}
