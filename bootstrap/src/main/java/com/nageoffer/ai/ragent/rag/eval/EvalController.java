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

package com.nageoffer.ai.ragent.rag.eval;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.nageoffer.ai.ragent.framework.convention.Result;
import com.nageoffer.ai.ragent.framework.convention.RetrievedChunk;
import com.nageoffer.ai.ragent.framework.web.Results;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeChunkDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeChunkMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.nageoffer.ai.ragent.rag.config.SearchChannelProperties;
import com.nageoffer.ai.ragent.rag.core.intent.IntentResolver;
import com.nageoffer.ai.ragent.rag.core.retrieve.RetrievalEngine;
import com.nageoffer.ai.ragent.rag.core.rewrite.QueryRewriteService;
import com.nageoffer.ai.ragent.rag.core.rewrite.RewriteResult;
import com.nageoffer.ai.ragent.rag.dto.RetrievalContext;
import com.nageoffer.ai.ragent.rag.dto.SubQuestionIntent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Evaluation-only endpoint that runs rewrite, intent resolution and retrieval
 * without generating an LLM answer.
 */
@RestController
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.eval", name = "enabled", havingValue = "true")
public class EvalController {

    private final QueryRewriteService queryRewriteService;
    private final IntentResolver intentResolver;
    private final RetrievalEngine retrievalEngine;
    private final SearchChannelProperties searchProperties;
    private final KnowledgeChunkMapper knowledgeChunkMapper;
    private final KnowledgeDocumentMapper knowledgeDocumentMapper;

    @GetMapping("/rag/eval")
    public Result<EvalResponse> evaluate(@RequestParam String question) {
        long start = System.currentTimeMillis();

        RewriteResult rewriteResult = queryRewriteService.rewriteWithSplit(question, List.of());
        List<SubQuestionIntent> subIntents = intentResolver.resolve(rewriteResult);
        RetrievalContext retrievalContext = retrievalEngine.retrieve(
                subIntents,
                searchProperties.getDefaultTopK()
        );

        return Results.success(buildResponse(
                retrievalContext,
                subIntents,
                System.currentTimeMillis() - start
        ));
    }

    private EvalResponse buildResponse(RetrievalContext context,
                                       List<SubQuestionIntent> subIntents,
                                       long latencyMs) {
        List<RetrievedChunk> uniqueChunks = flattenChunks(context);
        List<String> chunkIds = uniqueChunks.stream()
                .map(RetrievedChunk::getId)
                .filter(StrUtil::isNotBlank)
                .collect(Collectors.toList());
        List<String> contexts = uniqueChunks.stream()
                .map(RetrievedChunk::getText)
                .collect(Collectors.toList());

        List<String> contextDocIds = resolveContextDocIds(uniqueChunks);

        return EvalResponse.builder()
                .retrievedDocIds(dedupNonBlank(contextDocIds))
                .retrievedChunkIds(chunkIds)
                .retrievedContexts(contexts)
                .retrievedContextDocIds(contextDocIds)
                .mcpContext(context == null ? null : context.getMcpContext())
                .hasMcp(context != null && context.hasMcp())
                .hasKb(context != null && context.hasKb())
                .subIntents(extractSubIntents(subIntents))
                .intentLeafIds(extractTopLeafIds(subIntents))
                .latencyMs(latencyMs)
                .build();
    }

    /**
     * Flattens chunks grouped by intent and keeps the first occurrence of each
     * chunk ID.
     */
    private List<RetrievedChunk> flattenChunks(RetrievalContext context) {
        if (context == null || CollUtil.isEmpty(context.getIntentChunks())) {
            return Collections.emptyList();
        }
        Set<String> seen = new LinkedHashSet<>();
        return context.getIntentChunks().values().stream()
                .filter(CollUtil::isNotEmpty)
                .flatMap(List::stream)
                .filter(chunk -> chunk != null && StrUtil.isNotBlank(chunk.getId()))
                .filter(chunk -> seen.add(chunk.getId()))
                .collect(Collectors.toList());
    }

    /**
     * Resolves each chunk ID to the document file name without its extension.
     * Null placeholders are retained so this list stays aligned with contexts.
     */
    private List<String> resolveContextDocIds(List<RetrievedChunk> chunks) {
        if (CollUtil.isEmpty(chunks)) {
            return Collections.emptyList();
        }

        List<String> chunkIds = chunks.stream()
                .map(RetrievedChunk::getId)
                .filter(StrUtil::isNotBlank)
                .distinct()
                .collect(Collectors.toList());
        if (chunkIds.isEmpty()) {
            return new ArrayList<>(Collections.nCopies(chunks.size(), null));
        }

        Map<String, String> chunkToDocument = knowledgeChunkMapper.selectByIds(chunkIds).stream()
                .filter(chunk -> StrUtil.isNotBlank(chunk.getId()) && StrUtil.isNotBlank(chunk.getDocId()))
                .collect(Collectors.toMap(
                        KnowledgeChunkDO::getId,
                        KnowledgeChunkDO::getDocId,
                        (first, ignored) -> first
                ));

        List<String> documentIds = chunkToDocument.values().stream()
                .distinct()
                .collect(Collectors.toList());
        Map<String, String> documentToBusinessId = documentIds.isEmpty()
                ? Map.of()
                : knowledgeDocumentMapper.selectByIds(documentIds).stream()
                        .filter(document -> StrUtil.isNotBlank(document.getId())
                                && StrUtil.isNotBlank(document.getDocName()))
                        .collect(Collectors.toMap(
                                KnowledgeDocumentDO::getId,
                                document -> stripExtension(document.getDocName()),
                                (first, ignored) -> first
                        ));

        return chunks.stream()
                .map(chunk -> {
                    String documentId = chunkToDocument.get(chunk.getId());
                    return StrUtil.isBlank(documentId) ? null : documentToBusinessId.get(documentId);
                })
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private static String stripExtension(String documentName) {
        if (documentName == null) {
            return null;
        }
        int dot = documentName.lastIndexOf('.');
        return dot > 0 && dot < documentName.length() - 1
                ? documentName.substring(0, dot)
                : documentName;
    }

    private List<String> dedupNonBlank(List<String> values) {
        if (CollUtil.isEmpty(values)) {
            return Collections.emptyList();
        }
        Set<String> seen = new LinkedHashSet<>();
        return values.stream()
                .filter(StrUtil::isNotBlank)
                .filter(seen::add)
                .collect(Collectors.toList());
    }

    private List<String> extractSubIntents(List<SubQuestionIntent> intents) {
        if (CollUtil.isEmpty(intents)) {
            return Collections.emptyList();
        }
        return intents.stream()
                .map(SubQuestionIntent::subQuestion)
                .filter(StrUtil::isNotBlank)
                .collect(Collectors.toList());
    }

    private List<String> extractTopLeafIds(List<SubQuestionIntent> intents) {
        if (CollUtil.isEmpty(intents)) {
            return Collections.emptyList();
        }
        return intents.stream()
                .map(intent -> {
                    if (CollUtil.isEmpty(intent.nodeScores())) {
                        return null;
                    }
                    return intent.nodeScores().get(0).getNode().getId();
                })
                .collect(Collectors.toList());
    }
}
