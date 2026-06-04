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

package com.nageoffer.ai.ragent.mcp.tools;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Slf4j
@Component
public class TicketMcpExecutor {

    private static final String TOOL_ID = "ticket_query";
    private static final List<String> REGIONS = List.of("华东", "华南", "华北", "西南", "西北");
    private static final List<String> PRODUCTS = List.of("企业版", "专业版", "基础版");
    private static final List<String> STATUSES = List.of("待处理", "处理中", "已解决", "已关闭");
    private static final List<String> PRIORITIES = List.of("紧急", "高", "中", "低");

    @Tool(name = TOOL_ID, description = "Query customer support ticket data by region, status, priority, product and customer keyword.")
    public String queryTickets(
            @ToolParam(required = false, description = "Region filter: 华东, 华南, 华北, 西南, 西北.") String region,
            @ToolParam(required = false, description = "Ticket status: 待处理, 处理中, 已解决, 已关闭.") String status,
            @ToolParam(required = false, description = "Priority: 紧急, 高, 中, 低.") String priority,
            @ToolParam(required = false, description = "Product filter: 企业版, 专业版, 基础版.") String product,
            @ToolParam(required = false, description = "Customer name keyword.") String customerName,
            @ToolParam(required = false, description = "Query type: summary, list, stats. Defaults to summary.") String queryType,
            @ToolParam(required = false, description = "Maximum records to return. Defaults to 10.") Integer limit) {
        long startMs = System.currentTimeMillis();
        try {
            String normalizedQueryType = queryType == null || queryType.isBlank() ? "summary" : queryType;
            int normalizedLimit = limit == null || limit <= 0 ? 10 : Math.min(limit, 50);
            List<TicketRecord> data = generateMockData().stream()
                    .filter(t -> region == null || region.isBlank() || region.equals(t.region()))
                    .filter(t -> status == null || status.isBlank() || status.equals(t.status()))
                    .filter(t -> priority == null || priority.isBlank() || priority.equals(t.priority()))
                    .filter(t -> product == null || product.isBlank() || product.equals(t.product()))
                    .filter(t -> customerName == null || customerName.isBlank() || t.customer().contains(customerName))
                    .toList();

            String result = switch (normalizedQueryType) {
                case "list" -> buildListResult(data, normalizedLimit);
                case "stats" -> buildStatsResult(data);
                default -> buildSummaryResult(data);
            };
            log.info("Spring AI MCP tool call completed, toolId={}, queryType={}, elapsed={}ms",
                    TOOL_ID, normalizedQueryType, System.currentTimeMillis() - startMs);
            return result;
        } catch (Exception e) {
            log.error("Spring AI MCP tool call failed, toolId={}, elapsed={}ms",
                    TOOL_ID, System.currentTimeMillis() - startMs, e);
            return "工单查询失败: " + e.getMessage();
        }
    }

    private String buildSummaryResult(List<TicketRecord> data) {
        long pending = data.stream().filter(t -> "待处理".equals(t.status())).count();
        long inProgress = data.stream().filter(t -> "处理中".equals(t.status())).count();
        long resolved = data.stream().filter(t -> "已解决".equals(t.status())).count();
        long closed = data.stream().filter(t -> "已关闭".equals(t.status())).count();
        return """
                【客户工单汇总】
                工单总数: %d
                待处理: %d
                处理中: %d
                已解决: %d
                已关闭: %d
                """.formatted(data.size(), pending, inProgress, resolved, closed).trim();
    }

    private String buildListResult(List<TicketRecord> data, int limit) {
        return data.stream()
                .sorted(Comparator.comparingInt(t -> PRIORITIES.indexOf(t.priority())))
                .limit(limit)
                .map(t -> "%s | %s | %s | %s | %s | %s".formatted(
                        t.ticketId(), t.customer(), t.region(), t.product(), t.priority(), t.status()))
                .collect(Collectors.joining("\n", "【工单列表】\n", ""));
    }

    private String buildStatsResult(List<TicketRecord> data) {
        return data.stream()
                .collect(Collectors.groupingBy(TicketRecord::product, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .map(e -> e.getKey() + ": " + e.getValue() + " 个")
                .collect(Collectors.joining("\n", "【按产品统计】\n", ""));
    }

    private List<TicketRecord> generateMockData() {
        Random random = new Random(LocalDate.now().toEpochDay());
        List<TicketRecord> records = new ArrayList<>();
        for (int i = 1; i <= 80; i++) {
            records.add(new TicketRecord(
                    "TK-" + LocalDate.now().getYear() + "-" + String.format("%04d", i),
                    "客户" + (random.nextInt(20) + 1),
                    REGIONS.get(random.nextInt(REGIONS.size())),
                    PRODUCTS.get(random.nextInt(PRODUCTS.size())),
                    PRIORITIES.get(random.nextInt(PRIORITIES.size())),
                    STATUSES.get(random.nextInt(STATUSES.size()))
            ));
        }
        return records;
    }

    private record TicketRecord(String ticketId, String customer, String region, String product, String priority,
                                String status) {
    }
}
