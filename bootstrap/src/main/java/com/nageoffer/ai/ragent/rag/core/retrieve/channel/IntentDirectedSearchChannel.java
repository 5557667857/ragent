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

package com.nageoffer.ai.ragent.rag.core.retrieve.channel;

import cn.hutool.core.collection.CollUtil;
import com.nageoffer.ai.ragent.framework.convention.RetrievedChunk;
import com.nageoffer.ai.ragent.rag.config.SearchChannelProperties;
import com.nageoffer.ai.ragent.rag.core.intent.NodeScore;
import com.nageoffer.ai.ragent.rag.core.intent.NodeScoreFilters;
import com.nageoffer.ai.ragent.rag.core.retrieve.RetrieverService;
import com.nageoffer.ai.ragent.rag.core.retrieve.channel.strategy.IntentParallelRetriever;
import com.nageoffer.ai.ragent.rag.dto.SubQuestionIntent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * 意图定向检索通道
 * <p>
 * 基于意图识别结果，在特定知识库中进行定向检索
 * 这是最精确的检索方式，优先级最高
 */
@Slf4j
@Component
public class IntentDirectedSearchChannel implements SearchChannel {

    private final SearchChannelProperties properties;
    private final IntentParallelRetriever parallelRetriever;

    public IntentDirectedSearchChannel(RetrieverService retrieverService,
                                       SearchChannelProperties properties,
                                       Executor innerRetrievalExecutor) {
        this.properties = properties;
        this.parallelRetriever = new IntentParallelRetriever(retrieverService, innerRetrievalExecutor);
    }

    @Override
    public String getName() {
        return "IntentDirectedSearch";
    }

    @Override
    public int getPriority() {
        return 1;  // 最高优先级
    }

    @Override
    public boolean isEnabled(SearchContext context) {
        // 检查配置是否启用
        if (!properties.getChannels().getIntentDirected().isEnabled()) {
            return false;
        }

        // 检查是否有 KB 意图（而不仅仅是有意图）
        if (context.getSubIntent() == null) {
            return false;
        }

        // 提取 KB 意图，只有存在 KB 意图时才启用
        List<NodeScore> kbIntents = extractKbIntents(context);
        return CollUtil.isNotEmpty(kbIntents);
    }

    @Override
    public SearchChannelResult search(SearchContext context) {
        long startTime = System.currentTimeMillis();

        try {
            // 提取 KB 意图, 并过滤掉分数过低的意图
            List<NodeScore> kbIntents = extractKbIntents(context);

            // 并行检索所有意图对应的知识库
            int topKMultiplier = properties.getChannels().getIntentDirected().getTopKMultiplier();
            List<RetrievedChunk> allChunks = retrieveByIntents(
                    context.getMainQuestion(),
                    kbIntents,
                    context.getTopK(),
                    topKMultiplier
            );

            long latency = System.currentTimeMillis() - startTime;

            log.info("意图定向检索完成，检索到 {} 个 Chunk，耗时 {}ms",
                    allChunks.size(), latency);

            return SearchChannelResult.builder()
                    .channelType(SearchChannelType.INTENT_DIRECTED)
                    .channelName(getName())
                    .chunks(allChunks)
                    .latencyMs(latency)
                    .metadata(Map.of("intentCount", kbIntents.size()))
                    .build();

        } catch (Exception e) {
            log.error("意图定向检索失败", e);
            return SearchChannelResult.builder()
                    .channelType(SearchChannelType.INTENT_DIRECTED)
                    .channelName(getName())
                    .chunks(List.of())
                    .latencyMs(System.currentTimeMillis() - startTime)
                    .build();
        }
    }

    @Override
    public SearchChannelType getType() {
        return SearchChannelType.INTENT_DIRECTED;
    }

    /**
     * 提取 KB 意图
     */
    private List<NodeScore> extractKbIntents(SearchContext context) {
        // 检查 KB 意图分数阈值，过滤掉分数过低的意图
        double minScore = properties.getChannels().getIntentDirected().getMinIntentScore();
        SubQuestionIntent subIntent = context.getSubIntent();
        List<NodeScore> allScores = subIntent != null ? subIntent.nodeScores() : List.of();
        return NodeScoreFilters.kb(allScores, minScore);
    }

    /**
     * 根据意图列表并行检索
     */
    private List<RetrievedChunk> retrieveByIntents(String question,
                                                   List<NodeScore> kbIntents,
                                                   int fallbackTopK,
                                                   int topKMultiplier) {
        // 使用模板方法执行并行检索
        return parallelRetriever.executeParallelRetrieval(question, kbIntents, fallbackTopK, topKMultiplier);
    }
}
