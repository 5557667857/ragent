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
 * 妫€绱㈠紩鎿庛€?
 * <p>
 * 璐熻矗鍗忚皟鐭ヨ瘑搴撴绱㈠拰 MCP 宸ュ叿璋冪敤锛屽皢涓嶅悓閫氶亾鐨勭粨鏋滄牸寮忓寲涓哄彲渚?LLM 浣跨敤鐨勪笂涓嬫枃銆?
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
     * 鏍规嵁瀛愰棶棰樻剰鍥惧垪琛ㄦ墽琛屾绱紝鏁村悎鐭ヨ瘑搴撳拰 MCP 宸ュ叿缁撴灉銆?
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
                                log.error("瀛愰棶棰樹笂涓嬫枃鏋勫缓澶辫触锛岄檷绾т负绌轰笂涓嬫枃锛宷uestion={}", si.subQuestion(), e);
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
     * 鏍规嵁鍗曚釜瀛愰棶棰樼殑鎰忓浘鍒嗙被缁撴灉锛屽垎鍒墽琛?KB 妫€绱㈠拰 MCP 宸ュ叿璋冪敤锛屽苟鏋勫缓璇ュ瓙闂涓婁笅鏂囥€?
     * <p>
     * 鎰忓浘鍒板姩浣滅殑鍒嗗彂鍦ㄨ繖閲屽彂鐢燂細涓€涓瓙闂鍙兘鍚屾椂鍛戒腑 KB 鍜?MCP 涓ょ被鎰忓浘锛?
     * 涓や釜閫氶亾浼氬垎鍒墽琛屽悗鍐嶅悎骞躲€?
     *
     * @param intent 瀛愰棶棰樺強鍏舵剰鍥惧€欓€夊垪琛?
     * @param topK   璇ュ瓙闂鐨勬绱?TopK锛屾湭閰嶇疆鏃跺洖閫€鍒板叏灞€榛樿鍊?
     * @return 瀛愰棶棰樹笂涓嬫枃锛屽寘鍚?KB 妫€绱㈡枃鏈€丮CP 璋冪敤缁撴灉鏂囨湰锛屼互鍙婃寜鎰忓浘鑺傜偣鍒嗙粍鐨勫師濮?chunk
     */
    private SubQuestionContext buildSubQuestionContext(SubQuestionIntent intent, int topK) {
        // 1. 鎸?IntentNode.kind 灏嗘剰鍥惧€欓€夊垎娴侊細KB 璧扮煡璇嗗簱妫€绱紝MCP 璧板伐鍏疯皟鐢ㄣ€?
        //    NodeScoreFilters.kb(): node != null && node.isKB()
        //    NodeScoreFilters.mcp(): node != null && node.isMCP() && mcpToolId 闈炵┖
        List<NodeScore> kbIntents = NodeScoreFilters.kb(intent.nodeScores());
        List<NodeScore> mcpIntents = NodeScoreFilters.mcp(intent.nodeScores());

        // 2. 鐭ヨ瘑搴撻€氶亾锛氭墽琛屽閫氶亾妫€绱€侀噸鎺掑拰涓婁笅鏂囨牸寮忓寲銆?
        //    杩斿洖 KbResult锛屽寘鍚牸寮忓寲鏂囨湰 groupedContext 鍜屾寜鎰忓浘鑺傜偣鍒嗙粍鐨勫師濮?chunk銆?
        KbResult kbResult = retrieveAndRerank(intent, kbIntents, topK);

        // 3. MCP 閫氶亾锛氬鏋滃懡涓?MCP 鎰忓浘锛屽垯鎸夋剰鍥捐妭鐐归€愪釜璋冪敤宸ュ叿骞跺悎骞剁粨鏋溿€?
        //    executeMcpAndMerge 鍐呴儴浼氾細
        //    a) 閬嶅巻姣忎釜 MCP 鎰忓浘骞惰皟鐢?executeSingleMcpTool()
        //    b) 灏嗗懡涓殑 ToolCallback 浜ょ粰 ChatClient锛岀敱妯″瀷鏍规嵁宸ュ叿 schema 瑙ｆ瀽鍙傛暟骞惰皟鐢ㄥ伐鍏?
        //    c) 浣跨敤 ContextFormatter 灏嗘ā鍨嬬粨鍚堝伐鍏风粨鏋滅敓鎴愮殑鏂囨湰鏍煎紡鍖栦负 LLM 鍙涓婁笅鏂?
        String mcpContext = CollUtil.isNotEmpty(mcpIntents)
                ? executeMcpAndMerge(intent.subQuestion(), mcpIntents)
                : "";

        // 4. 鍚堝苟涓よ矾缁撴灉涓轰竴涓?SubQuestionContext銆?
        return new SubQuestionContext(intent.subQuestion(), kbResult.groupedContext(), mcpContext, kbResult.intentChunks());
    }

    /**
     * 璁＄畻瀛愰棶棰樺疄闄呬娇鐢ㄧ殑 TopK銆?
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
        // 浣跨敤澶氶€氶亾妫€绱㈠紩鎿庯紝鏄惁鍚敤鍏ㄥ眬妫€绱㈢敱缃俊搴﹂槇鍊煎喅瀹氥€?
        List<RetrievedChunk> chunks = multiChannelRetrievalEngine.retrieveKnowledgeChannels(intent, topK);

        if (CollUtil.isEmpty(chunks)) {
            return KbResult.empty();
        }

        // 鎸夋剰鍥捐妭鐐瑰垎缁勶紝鐢ㄤ簬鏍煎紡鍖栦笂涓嬫枃銆?
        Map<String, List<RetrievedChunk>> intentChunks = new HashMap<>();

        // 濡傛灉鏈夋剰鍥捐瘑鍒粨鏋滐紝鎸夋剰鍥捐妭鐐?ID 鍒嗙粍銆?
        if (CollUtil.isNotEmpty(kbIntents)) {
            // 澶氶€氶亾妫€绱㈣繑鍥炵殑 chunks 鏃犳硶绮剧‘瀵瑰簲鍒版煇涓剰鍥捐妭鐐癸紝
            // 鍥犳灏嗘墍鏈?chunks 鍒嗛厤缁欐瘡涓懡涓殑鎰忓浘鑺傜偣銆?
            for (NodeScore ns : kbIntents) {
                intentChunks.put(ns.getNode().getId(), chunks);
            }
        } else {
            // 濡傛灉娌℃湁鎰忓浘璇嗗埆缁撴灉锛屼娇鐢ㄧ壒娈?key 鎵胯浇澶氶€氶亾妫€绱㈢粨鏋溿€?
            intentChunks.put(MULTI_CHANNEL_KEY, chunks);
        }

        String groupedContext = contextFormatter.formatKbContext(kbIntents, intentChunks, topK);
        return new KbResult(groupedContext, intentChunks);
    }

    /**
     * 鎵ц MCP 宸ュ叿璋冪敤锛岃繑鍥炴寜 toolId 鍒嗙粍鐨勭粨鏋溿€?
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
                                log.warn("MCP tool call failed, ignore tool result, toolId={}, error={}",
                                        toolId, e.getMessage());
                                return null;
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
        // 浠庢剰鍥捐妭鐐逛腑鍙栧緱瑕佽皟鐢ㄧ殑 MCP 宸ュ叿鍚嶃€?
        String toolId = intentNode.getMcpToolId();
        // 鎸夊伐鍏峰悕鑾峰彇 Spring AI MCP ToolCallback銆傝繖閲岃繑鍥炵殑 ToolCallback 鍐呴儴鎸佹湁 MCP Client
        Optional<ToolCallback> toolCallbackOpt = mcpToolRegistry.getToolCallback(toolId);
        if (toolCallbackOpt.isEmpty()) {
            log.warn("MCP tool does not exist or is unavailable, ignore MCP context, toolId={}", toolId);
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
