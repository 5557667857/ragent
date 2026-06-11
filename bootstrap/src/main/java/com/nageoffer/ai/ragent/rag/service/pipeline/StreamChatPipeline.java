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

package com.nageoffer.ai.ragent.rag.service.pipeline;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.nageoffer.ai.ragent.framework.convention.ChatMessage;
import com.nageoffer.ai.ragent.framework.convention.ChatRequest;
import com.nageoffer.ai.ragent.infra.chat.StreamCallback;
import com.nageoffer.ai.ragent.infra.chat.StreamCancellationHandle;
import com.nageoffer.ai.ragent.infra.chat.StreamCancellationHandles;
import com.nageoffer.ai.ragent.rag.core.guidance.GuidanceDecision;
import com.nageoffer.ai.ragent.rag.core.guidance.IntentGuidanceService;
import com.nageoffer.ai.ragent.rag.core.intent.IntentResolver;
import com.nageoffer.ai.ragent.rag.core.llm.SpringAiChatSupport;
import com.nageoffer.ai.ragent.rag.core.memory.ConversationMemoryService;
import com.nageoffer.ai.ragent.rag.core.prompt.PromptContext;
import com.nageoffer.ai.ragent.rag.core.prompt.PromptTemplateLoader;
import com.nageoffer.ai.ragent.rag.core.prompt.RAGPromptService;
import com.nageoffer.ai.ragent.rag.core.retrieve.RetrievalEngine;
import com.nageoffer.ai.ragent.rag.core.rewrite.QueryRewriteService;
import com.nageoffer.ai.ragent.rag.core.rewrite.RewriteResult;
import com.nageoffer.ai.ragent.rag.dto.IntentGroup;
import com.nageoffer.ai.ragent.rag.dto.RetrievalContext;
import com.nageoffer.ai.ragent.rag.dto.SubQuestionIntent;
import com.nageoffer.ai.ragent.rag.config.SearchChannelProperties;
import com.nageoffer.ai.ragent.rag.service.handler.StreamTaskManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.CHAT_SYSTEM_PROMPT_PATH;

/**
 * 流式对话流水线
 * <p>
 * 承载从 RAGChatServiceImpl 提取的业务编排逻辑：
 * 记忆加载 -> 改写拆分 -> 意图解析 -> 歧义引导 -> 系统响应 / 检索 -> Prompt 组装 -> 流式输出
 * <p>
 * 流水线模式：通过私有方法 + boolean 返回值（handleXxx 返回 true 表示已处理并短路）
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StreamChatPipeline {

    private final SearchChannelProperties searchProperties;
    private final ConversationMemoryService memoryService;
    private final QueryRewriteService queryRewriteService;
    private final IntentResolver intentResolver;
    private final IntentGuidanceService guidanceService;
    private final RetrievalEngine retrievalEngine;
    private final ChatModel chatModel;
    private final RAGPromptService promptBuilder;
    private final PromptTemplateLoader promptTemplateLoader;
    private final StreamTaskManager taskManager;

    /**
     * 执行流式对话管道
     */
    /**
     * 执行流式对话管道的主入口方法
     * @param ctx 流式对话上下文，包含请求参数、状态及回调等信息
     */
    public void execute(StreamChatContext ctx) {
        long pipelineStart = System.currentTimeMillis();

        // 加载历史对话记忆并追加当前用户问题到上下文中
        long t0 = System.currentTimeMillis();
        loadMemory(ctx);
        log.info("[流水线耗时] loadMemory: {}ms", System.currentTimeMillis() - t0);

        // 对用户问题进行查询改写和子问题拆分
        long t1 = System.currentTimeMillis();
        rewriteQuery(ctx);
        log.info("[流水线耗时] rewriteQuery: {}ms", System.currentTimeMillis() - t1);

        // 解析改写后问题的意图，识别具体的业务或知识领域意图
        long t2 = System.currentTimeMillis();
        resolveIntents(ctx);
        log.info("[流水线耗时] resolveIntents: {}ms", System.currentTimeMillis() - t2);

        // 检测是否存在歧义，若存在则进行引导性回复并终止后续流程
        long t3 = System.currentTimeMillis();
        if (handleGuidance(ctx)) {
            log.info("[流水线耗时] handleGuidance 短路, 总耗时: {}ms", System.currentTimeMillis() - pipelineStart);
            return;
        }
        log.info("[流水线耗时] handleGuidance: {}ms", System.currentTimeMillis() - t3);

        // 检查是否仅包含系统级意图（如问候、闲聊等），若是则直接生成系统回复并终止后续流程
        long t4 = System.currentTimeMillis();
        if (handleSystemOnly(ctx)) {
            log.info("[流水线耗时] handleSystemOnly 短路, 总耗时: {}ms", System.currentTimeMillis() - pipelineStart);
            return;
        }
        log.info("[流水线耗时] handleSystemOnly: {}ms", System.currentTimeMillis() - t4);

        // 根据解析出的意图执行知识库或外部工具的检索操作
        long t5 = System.currentTimeMillis();
        RetrievalContext retrievalCtx = retrieve(ctx);
        log.info("[流水线耗时] retrieve: {}ms", System.currentTimeMillis() - t5);

        // 若检索结果为空，则返回默认提示语并终止后续流程
        if (handleEmptyRetrieval(ctx, retrievalCtx)) {
            log.info("[流水线耗时] handleEmptyRetrieval 短路, 总耗时: {}ms", System.currentTimeMillis() - pipelineStart);
            return;
        }

        // 组装最终 Prompt 并调用大模型进行流式响应输出
        long t6 = System.currentTimeMillis();
        streamRagResponse(ctx, retrievalCtx);
        log.info("[流水线耗时] streamRagResponse 启动: {}ms (总耗时: {}ms)",
                System.currentTimeMillis() - t6, System.currentTimeMillis() - pipelineStart);
    }

    // ==================== 流水线阶段 ====================

    private void loadMemory(StreamChatContext ctx) {
        List<ChatMessage> history = memoryService.loadAndAppend(
                ctx.getConversationId(),
                ctx.getUserId(),
                ChatMessage.user(ctx.getQuestion())
        );
        ctx.setHistory(history);
    }

    private void rewriteQuery(StreamChatContext ctx) {
        RewriteResult rewriteResult = queryRewriteService.rewriteWithSplit(ctx.getQuestion(), ctx.getHistory());
        ctx.setRewriteResult(rewriteResult);
    }

    private void resolveIntents(StreamChatContext ctx) {
        List<SubQuestionIntent> subIntents = intentResolver.resolve(ctx.getRewriteResult());
        ctx.setSubIntents(subIntents);
    }

    private boolean handleGuidance(StreamChatContext ctx) {
        // 意图识别后先判断是否存在歧义：例如多个 KB 意图分数很接近，
        // 此时不急着检索，而是先让用户确认具体想问哪个方向。
        GuidanceDecision decision = guidanceService.detectAmbiguity(
                ctx.getRewriteResult().rewrittenQuestion(),
                ctx.getSubIntents()
        );
        // 没有歧义时放行，继续后面的系统意图判断、知识库检索或 MCP 工具调用。
        if (!decision.isPrompt()) {
            return false;
        }
        // 有歧义时直接把澄清问题通过流式回调返回给前端，并结束本轮处理。
        // 返回 true 表示当前阶段已经处理完，execute() 会短路，不再继续检索和生成答案。
        StreamCallback callback = ctx.getCallback();
        callback.onContent(decision.getPrompt());
        callback.onComplete();
        return true;
    }

    private boolean handleSystemOnly(StreamChatContext ctx) {
        List<SubQuestionIntent> subIntents = ctx.getSubIntents();
        boolean allSystemOnly = subIntents.stream()
                .allMatch(si -> intentResolver.isSystemOnly(si.nodeScores()));
        if (!allSystemOnly) {
            return false;
        }
        String customPrompt = subIntents.stream()
                .flatMap(si -> si.nodeScores().stream())
                .map(ns -> ns.getNode().getPromptTemplate())
                .filter(StrUtil::isNotBlank)
                .findFirst()
                .orElse(null);
        StreamCancellationHandle handle = streamSystemResponse(
                ctx.getRewriteResult().rewrittenQuestion(),
                ctx.getHistory(),
                customPrompt,
                ctx.getCallback()
        );
        taskManager.bindHandle(ctx.getTaskId(), handle);
        return true;
    }

    private RetrievalContext retrieve(StreamChatContext ctx) {
        return retrievalEngine.retrieve(ctx.getSubIntents(), searchProperties.getDefaultTopK());
    }

    private boolean handleEmptyRetrieval(StreamChatContext ctx, RetrievalContext retrievalCtx) {
        if (!retrievalCtx.isEmpty()) {
            return false;
        }
        StreamCallback callback = ctx.getCallback();
        callback.onContent("未检索到与问题相关的文档内容。");
        callback.onComplete();
        return true;
    }

    private void streamRagResponse(StreamChatContext ctx, RetrievalContext retrievalCtx) {
        // 对已经过滤的意图进行mcp和kb的划分
        IntentGroup mergedGroup = intentResolver.mergeIntentGroup(ctx.getSubIntents());

        StreamCancellationHandle handle = streamLLMResponse(
                ctx.getRewriteResult(),
                retrievalCtx,
                mergedGroup,
                ctx.getHistory(),
                ctx.isDeepThinking(),
                ctx.getCallback()
        );
        taskManager.bindHandle(ctx.getTaskId(), handle);
    }

    // ==================== LLM 响应 ====================

    private StreamCancellationHandle streamSystemResponse(String question, List<ChatMessage> history,
                                                          String customPrompt, StreamCallback callback) {
        String systemPrompt = StrUtil.isNotBlank(customPrompt)
                ? customPrompt
                : promptTemplateLoader.load(CHAT_SYSTEM_PROMPT_PATH);

        List<ChatMessage> messages = new ArrayList<>();
        messages.add(ChatMessage.system(systemPrompt));
        if (CollUtil.isNotEmpty(history)) {
            messages.addAll(history);
        }
        messages.add(ChatMessage.user(question));

        ChatRequest req = ChatRequest.builder()
                .messages(messages)
                .temperature(0.7D)
                .thinking(false)
                .build();
        return streamChat(req, callback);
    }

    private StreamCancellationHandle streamLLMResponse(RewriteResult rewriteResult, RetrievalContext ctx,
                                                       IntentGroup intentGroup, List<ChatMessage> history,
                                                       boolean deepThinking, StreamCallback callback) {
        PromptContext promptContext = PromptContext.builder()
                .question(rewriteResult.rewrittenQuestion())
                .mcpContext(ctx.getMcpContext())
                .kbContext(ctx.getKbContext())
                .mcpIntents(intentGroup.mcpIntents())
                .kbIntents(intentGroup.kbIntents())
                .intentChunks(ctx.getIntentChunks())
                .build();

        List<ChatMessage> messages = promptBuilder.buildStructuredMessages(
                promptContext,
                history,
                rewriteResult.rewrittenQuestion(),
                rewriteResult.subQuestions()  // 传入子问题列表
        );
        ChatRequest chatRequest = ChatRequest.builder()
                .messages(messages)
                .thinking(deepThinking)
                .temperature(ctx.hasMcp() ? 0.3D : 0D)  // MCP 场景稍微放宽温度
                .topP(ctx.hasMcp() ? 0.8D : 1D)
                .build();

        return streamChat(chatRequest, callback);
    }

    private StreamCancellationHandle streamChat(ChatRequest request, StreamCallback callback) {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        Disposable disposable = chatModel.stream(SpringAiChatSupport.toPrompt(request))
                .map(SpringAiChatSupport::content)
                .filter(StrUtil::isNotBlank)
                .subscribe(
                        callback::onContent,
                        callback::onError,
                        callback::onComplete
                );
        return StreamCancellationHandles.fromRunnable(disposable::dispose, cancelled);
    }
}
