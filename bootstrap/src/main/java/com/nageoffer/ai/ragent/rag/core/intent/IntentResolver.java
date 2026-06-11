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

package com.nageoffer.ai.ragent.rag.core.intent;

import cn.hutool.core.collection.CollUtil;
import com.nageoffer.ai.ragent.rag.dto.IntentCandidate;
import com.nageoffer.ai.ragent.rag.dto.IntentGroup;
import com.nageoffer.ai.ragent.rag.dto.SubQuestionIntent;
import com.nageoffer.ai.ragent.framework.trace.RagTraceNode;
import com.nageoffer.ai.ragent.rag.core.rewrite.RewriteResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.INTENT_MIN_SCORE;
import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.MAX_INTENT_COUNT;
import static com.nageoffer.ai.ragent.rag.enums.IntentKind.SYSTEM;

@Slf4j
@Service
@RequiredArgsConstructor
public class IntentResolver {

    @Qualifier("defaultIntentClassifier")
    private final IntentClassifier intentClassifier;
    @Qualifier("intentClassifyExecutor")
    private final ExecutorService intentClassifyExecutor;

    @RagTraceNode(name = "intent-resolve", type = "INTENT")
    public List<SubQuestionIntent> resolve(RewriteResult rewriteResult) {
        long start = System.currentTimeMillis();
        List<String> subQuestions = CollUtil.isNotEmpty(rewriteResult.subQuestions())
                ? rewriteResult.subQuestions()
                : List.of(rewriteResult.rewrittenQuestion());

        log.info("[意图解析耗时] 子问题数量: {}", subQuestions.size());

        List<Future<SubQuestionIntent>> futures = new ArrayList<>();
        for (String q : subQuestions) {
            futures.add(intentClassifyExecutor.submit(() -> {
                try {
                    return new SubQuestionIntent(q, classifyIntents(q));
                } catch (Exception e) {
                    log.error("子问题意图分类失败，降级为空意图，question：{}", q, e);
                    return new SubQuestionIntent(q, List.of());
                }
            }));
        }

        long t0 = System.currentTimeMillis();
        List<SubQuestionIntent> subIntents = futures.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        Thread.currentThread().interrupt();
                        log.error("获取意图分类结果异常", e);
                        return null;
                    }
                })
                .filter(item -> item != null)
                .collect(Collectors.toList());
        log.info("[意图解析耗时] 等待所有子问题意图分类完成: {}ms", System.currentTimeMillis() - t0);

        // 限制总意图数量不超过 MAX_INTENT_COUNT
        List<SubQuestionIntent> result = capTotalIntents(subIntents);
        log.info("[意图解析耗时] 总耗时: {}ms (子问题数: {})", System.currentTimeMillis() - start, subQuestions.size());
        return result;
    }

    public IntentGroup mergeIntentGroup(List<SubQuestionIntent> subIntents) {
        List<NodeScore> mcpIntents = new ArrayList<>();
        List<NodeScore> kbIntents = new ArrayList<>();
        for (SubQuestionIntent si : subIntents) {
            mcpIntents.addAll(NodeScoreFilters.mcp(si.nodeScores()));
            kbIntents.addAll(NodeScoreFilters.kb(si.nodeScores()));
        }
        return new IntentGroup(mcpIntents, kbIntents);
    }

    public boolean isSystemOnly(List<NodeScore> nodeScores) {
        return nodeScores.size() == 1
                && nodeScores.get(0).getNode() != null
                && nodeScores.get(0).getNode().getKind() == SYSTEM;
    }

    private List<NodeScore> classifyIntents(String question) {
        //子问题意图分类,子问题取分数最高的意图，最多三个，且分数必须大于等于 INTENT_MIN_SCORE
        List<NodeScore> scores = intentClassifier.classifyTargets(question);
        return scores.stream()
                .filter(ns -> ns.getScore() >= INTENT_MIN_SCORE)
                .limit(MAX_INTENT_COUNT)
                .toList();
    }

    /**
     * 限制总意图数量不超过 MAX_INTENT_COUNT
     * <p>
     * 策略：
     * 1. 如果总数未超限，直接返回
     * 2. 如果超限，每个子问题至少保留 1 个最高分意图
     * 3. 剩余配额按分数从高到低分配给其他意图
     */
    private List<SubQuestionIntent> capTotalIntents(List<SubQuestionIntent> subIntents) {
        int totalIntents = subIntents.stream()
                .mapToInt(si -> si.nodeScores().size())
                .sum();

        // 未超限，直接返回
        if (totalIntents <= MAX_INTENT_COUNT) {
            return subIntents;
        }

        // 步骤1：收集所有意图，按子问题索引分组
        List<IntentCandidate> allCandidates = collectAllCandidates(subIntents);

        // 步骤2：每个子问题保留最高分意图
        List<IntentCandidate> guaranteedIntents = selectTopIntentPerSubQuestion(allCandidates, subIntents.size());

        // 步骤3：计算剩余配额
        int remaining = MAX_INTENT_COUNT - guaranteedIntents.size();

        // 步骤4：从剩余候选中按分数选择
        List<IntentCandidate> additionalIntents = selectAdditionalIntents(allCandidates, guaranteedIntents, remaining);

        // 步骤5：合并并重建结果
        return rebuildSubIntents(subIntents, guaranteedIntents, additionalIntents);
    }

    /**
     * 收集所有意图候选，标记所属子问题索引
     */
    private List<IntentCandidate> collectAllCandidates(List<SubQuestionIntent> subIntents) {
        List<IntentCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < subIntents.size(); i++) {
            List<NodeScore> nodeScores = subIntents.get(i).nodeScores();
            if (CollUtil.isEmpty(nodeScores)) {
                continue;
            }
            for (NodeScore ns : nodeScores) {
                candidates.add(new IntentCandidate(i, ns));
            }
        }
        // 按分数降序排序
        candidates.sort((a, b) -> Double.compare(b.nodeScore().getScore(), a.nodeScore().getScore()));
        return candidates;
    }

    /**
     * 每个子问题选择最高分意图（保底策略）
     */
    private List<IntentCandidate> selectTopIntentPerSubQuestion(List<IntentCandidate> allCandidates, int subQuestionCount) {
        List<IntentCandidate> topIntents = new ArrayList<>();
        Set<Integer> seen = new HashSet<>();

        for (IntentCandidate candidate : allCandidates) {
            if (seen.add(candidate.subQuestionIndex())) {
                topIntents.add(candidate);
                if (topIntents.size() == subQuestionCount) {
                    break;
                }
            }
        }
        return topIntents;
    }

    /**
     * 从剩余候选中选择额外意图
     */
    private List<IntentCandidate> selectAdditionalIntents(List<IntentCandidate> allCandidates,
                                                          List<IntentCandidate> guaranteedIntents,
                                                          int remaining) {
        if (remaining <= 0) {
            return List.of();
        }

        List<IntentCandidate> additional = new ArrayList<>();
        for (IntentCandidate candidate : allCandidates) {
            // 跳过已经被选为保底的意图
            if (guaranteedIntents.contains(candidate)) {
                continue;
            }
            additional.add(candidate);
            if (additional.size() >= remaining) {
                break;
            }
        }
        return additional;
    }

    /**
     * 根据选中的意图重建 SubQuestionIntent 列表
     */
    private List<SubQuestionIntent> rebuildSubIntents(List<SubQuestionIntent> originalSubIntents,
                                                      List<IntentCandidate> guaranteedIntents,
                                                      List<IntentCandidate> additionalIntents) {
        // 合并所有选中的意图
        List<IntentCandidate> allSelected = new ArrayList<>(guaranteedIntents);
        allSelected.addAll(additionalIntents);

        // 按子问题索引分组
        Map<Integer, List<NodeScore>> groupedByIndex = new HashMap<>();
        for (IntentCandidate candidate : allSelected) {
            groupedByIndex.computeIfAbsent(candidate.subQuestionIndex(), k -> new ArrayList<>())
                    .add(candidate.nodeScore());
        }

        // 重建结果
        List<SubQuestionIntent> result = new ArrayList<>();
        for (int i = 0; i < originalSubIntents.size(); i++) {
            SubQuestionIntent original = originalSubIntents.get(i);
            List<NodeScore> retained = groupedByIndex.getOrDefault(i, List.of());
            result.add(new SubQuestionIntent(original.subQuestion(), retained));
        }
        return result;
    }
}
