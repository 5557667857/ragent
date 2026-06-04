package com.nageoffer.ai.ragent.rag.core.retrieve;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.nageoffer.ai.ragent.framework.convention.RetrievedChunk;
import com.nageoffer.ai.ragent.framework.trace.RagTraceNode;
import com.nageoffer.ai.ragent.rag.config.SearchChannelProperties;
import com.nageoffer.ai.ragent.rag.core.intent.IntentNode;
import com.nageoffer.ai.ragent.rag.core.intent.NodeScore;
import com.nageoffer.ai.ragent.rag.core.intent.NodeScoreFilters;
import com.nageoffer.ai.ragent.rag.core.mcp.McpToolRegistry;
import com.nageoffer.ai.ragent.rag.core.prompt.ContextFormatter;
import com.nageoffer.ai.ragent.rag.core.prompt.PromptTemplateLoader;
import com.nageoffer.ai.ragent.rag.dto.KbResult;
import com.nageoffer.ai.ragent.rag.dto.RetrievalContext;
import com.nageoffer.ai.ragent.rag.dto.SubQuestionIntent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.CONTEXT_FORMAT_PATH;
import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.MULTI_CHANNEL_KEY;

/**
 * 检索引擎。
 * <p>
 * 负责协调知识库检索和 MCP 工具调用，将不同通道的结果格式化为可供 LLM 使用的上下文。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RetrievalEngine {

    private final SearchChannelProperties searchProperties;
    private final ContextFormatter contextFormatter;
    private final PromptTemplateLoader templateLoader;
    private final McpToolRegistry mcpToolRegistry;
    private final MultiChannelRetrievalEngine multiChannelRetrievalEngine;
    private final ChatModel chatModel;
    private final Executor ragContextExecutor;
    private final Executor mcpBatchExecutor;

    /**
     * 根据子问题意图列表执行检索，整合知识库和 MCP 工具结果。
     */
    @RagTraceNode(name = "retrieval-engine", type = "RETRIEVE")
    public RetrievalContext retrieve(List<SubQuestionIntent> subIntents, int topK) {
        if (CollUtil.isEmpty(subIntents)) {
            return RetrievalContext.builder()
                    .intentChunks(Map.of())
                    .build();
        }

        int finalTopK = topK > 0 ? topK : searchProperties.getDefaultTopK();
        List<CompletableFuture<SubQuestionContext>> tasks = subIntents.stream()
                .map(si -> CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return buildSubQuestionContext(
                                        si,
                                        resolveSubQuestionTopK(si, finalTopK)
                                );
                            } catch (Exception e) {
                                log.error("子问题上下文构建失败，降级为空上下文，question={}", si.subQuestion(), e);
                                return new SubQuestionContext(si.subQuestion(), "", "", Map.of());
                            }
                        },
                        ragContextExecutor
                ))
                .toList();
        List<SubQuestionContext> contexts = tasks.stream()
                .map(CompletableFuture::join)
                .toList();

        Map<String, List<RetrievedChunk>> mergedIntentChunks = new HashMap<>();
        for (SubQuestionContext context : contexts) {
            if (CollUtil.isNotEmpty(context.intentChunks())) {
                mergedIntentChunks.putAll(context.intentChunks());
            }
        }

        boolean singleQuestion = contexts.size() == 1;
        String kbContext;
        String mcpContext;

        if (singleQuestion) {
            SubQuestionContext only = contexts.get(0);
            kbContext = StrUtil.emptyIfNull(only.kbContext()).trim();
            mcpContext = StrUtil.emptyIfNull(only.mcpContext()).trim();
        } else {
            StringBuilder kbBuilder = new StringBuilder();
            StringBuilder mcpBuilder = new StringBuilder();
            int globalIndex = 0;
            for (SubQuestionContext context : contexts) {
                boolean hasKb = StrUtil.isNotBlank(context.kbContext());
                boolean hasMcp = StrUtil.isNotBlank(context.mcpContext());
                if (hasKb || hasMcp) {
                    globalIndex++;
                }
                if (hasKb) {
                    appendSection(kbBuilder, "sub-question-kb-wrapper", globalIndex, context.question(), context.kbContext());
                }
                if (hasMcp) {
                    appendSection(mcpBuilder, "sub-question-mcp-wrapper", globalIndex, context.question(), context.mcpContext());
                }
            }
            kbContext = kbBuilder.toString().trim();
            mcpContext = mcpBuilder.toString().trim();
        }

        return RetrievalContext.builder()
                .mcpContext(mcpContext)
                .kbContext(kbContext)
                .intentChunks(mergedIntentChunks)
                .build();
    }

    /**
     * 根据单个子问题的意图分类结果，分别执行 KB 检索和 MCP 工具调用，并构建该子问题上下文。
     * <p>
     * 意图到动作的分发在这里发生：一个子问题可能同时命中 KB 和 MCP 两类意图，
     * 两个通道会分别执行后再合并。
     *
     * @param intent 子问题及其意图候选列表
     * @param topK   该子问题的检索 TopK，未配置时回退到全局默认值
     * @return 子问题上下文，包含 KB 检索文本、MCP 调用结果文本，以及按意图节点分组的原始 chunk
     */
    private SubQuestionContext buildSubQuestionContext(SubQuestionIntent intent, int topK) {
        // 1. 按 IntentNode.kind 将意图候选分流：KB 走知识库检索，MCP 走工具调用。
        //    NodeScoreFilters.kb(): node != null && node.isKB()
        //    NodeScoreFilters.mcp(): node != null && node.isMCP() && mcpToolId 非空
        List<NodeScore> kbIntents = NodeScoreFilters.kb(intent.nodeScores());
        List<NodeScore> mcpIntents = NodeScoreFilters.mcp(intent.nodeScores());

        // 2. 知识库通道：执行多通道检索、重排和上下文格式化。
        //    返回 KbResult，包含格式化文本 groupedContext 和按意图节点分组的原始 chunk。
        KbResult kbResult = retrieveAndRerank(intent, kbIntents, topK);

        // 3. MCP 通道：如果命中 MCP 意图，则按意图节点逐个调用工具并合并结果。
        //    executeMcpAndMerge 内部会：
        //    a) 遍历每个 MCP 意图并调用 executeSingleMcpTool()
        //    b) 将命中的 ToolCallback 交给 ChatClient，由模型根据工具 schema 解析参数并调用工具
        //    c) 使用 ContextFormatter 将模型结合工具结果生成的文本格式化为 LLM 可读上下文
        String mcpContext = CollUtil.isNotEmpty(mcpIntents)
                ? executeMcpAndMerge(intent.subQuestion(), mcpIntents)
                : "";

        // 4. 合并两路结果为一个 SubQuestionContext。
        return new SubQuestionContext(intent.subQuestion(), kbResult.groupedContext(), mcpContext, kbResult.intentChunks());
    }

    /**
     * 计算子问题实际使用的 TopK。
     */
    private int resolveSubQuestionTopK(SubQuestionIntent intent, int fallbackTopK) {
        return NodeScoreFilters.kb(intent.nodeScores()).stream()
                .map(NodeScore::getNode)
                .filter(Objects::nonNull)
                .map(IntentNode::getTopK)
                .filter(Objects::nonNull)
                .filter(topK -> topK > 0)
                .max(Integer::compareTo)
                .orElse(fallbackTopK);
    }

    private void appendSection(StringBuilder builder, String section, int index, String question, String context) {
        if (!builder.isEmpty()) {
            builder.append("\n");
        }
        builder.append(templateLoader.renderSection(CONTEXT_FORMAT_PATH, section, Map.of(
                "index", String.valueOf(index),
                "question", question,
                "context", context
        )));
    }

    private String executeMcpAndMerge(String question, List<NodeScore> mcpIntents) {
        if (CollUtil.isEmpty(mcpIntents)) {
            return "";
        }

        Map<String, List<String>> toolResults = executeMcpTools(question, mcpIntents);
        if (toolResults.isEmpty()) {
            return "";
        }

        return contextFormatter.formatMcpContext(toolResults, mcpIntents);
    }

    private KbResult retrieveAndRerank(SubQuestionIntent intent, List<NodeScore> kbIntents, int topK) {
        // 使用多通道检索引擎，是否启用全局检索由置信度阈值决定。
        List<RetrievedChunk> chunks = multiChannelRetrievalEngine.retrieveKnowledgeChannels(intent, topK);

        if (CollUtil.isEmpty(chunks)) {
            return KbResult.empty();
        }

        // 按意图节点分组，用于格式化上下文。
        Map<String, List<RetrievedChunk>> intentChunks = new HashMap<>();

        // 如果有意图识别结果，按意图节点 ID 分组。
        if (CollUtil.isNotEmpty(kbIntents)) {
            // 多通道检索返回的 chunks 无法精确对应到某个意图节点，
            // 因此将所有 chunks 分配给每个命中的意图节点。
            for (NodeScore ns : kbIntents) {
                intentChunks.put(ns.getNode().getId(), chunks);
            }
        } else {
            // 如果没有意图识别结果，使用特殊 key 承载多通道检索结果。
            intentChunks.put(MULTI_CHANNEL_KEY, chunks);
        }

        String groupedContext = contextFormatter.formatKbContext(kbIntents, intentChunks, topK);
        return new KbResult(groupedContext, intentChunks);
    }

    /**
     * 执行 MCP 工具调用，返回按 toolId 分组的结果。
     */
    private Map<String, List<String>> executeMcpTools(String question,
                                                       List<NodeScore> mcpIntentScores) {
        if (CollUtil.isEmpty(mcpIntentScores)) {
            return Map.of();
        }

        List<CompletableFuture<ToolOutput>> futures = mcpIntentScores.stream()
                .map(ns -> CompletableFuture.supplyAsync(
                        () -> {
                            String toolId = ns.getNode().getMcpToolId();
                            try {
                                String result = executeSingleMcpTool(question, ns.getNode());
                                return result == null ? null : new ToolOutput(toolId, result);
                            } catch (Exception e) {
                                log.error("MCP 工具调用异常, toolId: {}", toolId, e);
                                return new ToolOutput(toolId, "工具调用异常: " + e.getMessage());
                            }
                        },
                        mcpBatchExecutor
                ))
                .toList();

        return futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(
                        ToolOutput::toolId,
                        Collectors.mapping(ToolOutput::result, Collectors.toList())
                ));
    }

    private String executeSingleMcpTool(String question, IntentNode intentNode) {
        // 从意图节点中取得要调用的 MCP 工具名。
        String toolId = intentNode.getMcpToolId();
        // 按工具名获取 Spring AI MCP ToolCallback。这里返回的 ToolCallback 内部持有 MCP Client
        Optional<ToolCallback> toolCallbackOpt = mcpToolRegistry.getToolCallback(toolId);
        if (toolCallbackOpt.isEmpty()) {
            log.warn("MCP 工具不存在, toolId={}", toolId);
            return null;
        }
        ToolCallback toolCallback = toolCallbackOpt.get();
        StringBuilder systemPrompt = new StringBuilder(StrUtil.isNotBlank(intentNode.getPromptTemplate())
                ? intentNode.getPromptTemplate()
                : "你正在处理一个已由意图树判定需要调用 MCP 工具的问题。必须使用当前提供的工具获取结果，然后基于工具返回内容用中文简洁回答。");
        if (StrUtil.isNotBlank(intentNode.getParamPromptTemplate())) {
            systemPrompt.append("\n\n工具参数提取要求：\n")
                    .append(intentNode.getParamPromptTemplate());
        }

        return ChatClient.builder(chatModel)
                .build()
                .prompt()
                .system(systemPrompt.toString())
                .user(question)
                .toolCallbacks(toolCallback)
                .call()
                .content();
    }

    private record ToolOutput(String toolId, String result) {
    }

    private record SubQuestionContext(String question,
                                      String kbContext,
                                      String mcpContext,
                                      Map<String, List<RetrievedChunk>> intentChunks) {
    }
}
