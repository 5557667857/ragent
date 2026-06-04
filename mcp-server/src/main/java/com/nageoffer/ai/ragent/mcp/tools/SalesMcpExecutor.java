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
public class SalesMcpExecutor {

    private static final String TOOL_ID = "sales_query";
    private static final List<String> REGIONS = List.of("华东", "华南", "华北", "西南", "西北");
    private static final List<String> PRODUCTS = List.of("企业版", "专业版", "基础版");

    @Tool(name = TOOL_ID, description = "Query software sales data by region, period, product, sales person and query type.")
    public String querySales(
            @ToolParam(required = false, description = "Region filter: 华东, 华南, 华北, 西南, 西北.") String region,
            @ToolParam(required = false, description = "Period: 本月, 上月, 本季度, 上季度, 本年. Defaults to 本月.") String period,
            @ToolParam(required = false, description = "Product filter: 企业版, 专业版, 基础版.") String product,
            @ToolParam(required = false, description = "Sales person name.") String salesPerson,
            @ToolParam(required = false, description = "Query type: summary, ranking, detail, trend. Defaults to summary.") String queryType,
            @ToolParam(required = false, description = "Maximum records to return. Defaults to 10.") Integer limit) {
        long startMs = System.currentTimeMillis();
        try {
            String normalizedPeriod = blankToDefault(period, "本月");
            String normalizedQueryType = blankToDefault(queryType, "summary");
            int normalizedLimit = normalizeLimit(limit);
            List<SalesRecord> data = generateMockData(normalizedPeriod).stream()
                    .filter(r -> region == null || region.isBlank() || region.equals(r.region()))
                    .filter(r -> product == null || product.isBlank() || product.equals(r.product()))
                    .filter(r -> salesPerson == null || salesPerson.isBlank() || salesPerson.equals(r.salesPerson()))
                    .toList();

            String result = switch (normalizedQueryType) {
                case "ranking" -> buildRankingResult(data, normalizedPeriod, normalizedLimit);
                case "detail" -> buildDetailResult(data, normalizedPeriod, normalizedLimit);
                case "trend" -> buildTrendResult(data, normalizedPeriod);
                default -> buildSummaryResult(data, normalizedPeriod);
            };
            log.info("Spring AI MCP tool call completed, toolId={}, queryType={}, elapsed={}ms",
                    TOOL_ID, normalizedQueryType, System.currentTimeMillis() - startMs);
            return result;
        } catch (Exception e) {
            log.error("Spring AI MCP tool call failed, toolId={}, elapsed={}ms",
                    TOOL_ID, System.currentTimeMillis() - startMs, e);
            return "销售数据查询失败: " + e.getMessage();
        }
    }

    private String buildSummaryResult(List<SalesRecord> data, String period) {
        double totalAmount = data.stream().mapToDouble(SalesRecord::amount).sum();
        return """
                【%s 销售数据汇总】
                销售额: %.2f 万
                订单数: %d
                平均客单价: %.2f 万
                """.formatted(period, totalAmount, data.size(), data.isEmpty() ? 0 : totalAmount / data.size()).trim();
    }

    private String buildRankingResult(List<SalesRecord> data, String period, int limit) {
        return data.stream()
                .collect(Collectors.groupingBy(SalesRecord::salesPerson, Collectors.summingDouble(SalesRecord::amount)))
                .entrySet()
                .stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .limit(limit)
                .map(e -> e.getKey() + ": " + String.format("%.2f 万", e.getValue()))
                .collect(Collectors.joining("\n", "【" + period + " 销售排名】\n", ""));
    }

    private String buildDetailResult(List<SalesRecord> data, String period, int limit) {
        return data.stream()
                .sorted(Comparator.comparingDouble(SalesRecord::amount).reversed())
                .limit(limit)
                .map(r -> "%s | %s | %s | %.2f 万 | %s".formatted(r.date(), r.region(), r.product(), r.amount(), r.salesPerson()))
                .collect(Collectors.joining("\n", "【" + period + " 销售明细】\n", ""));
    }

    private String buildTrendResult(List<SalesRecord> data, String period) {
        return data.stream()
                .collect(Collectors.groupingBy(r -> "第" + ((r.date().getDayOfMonth() - 1) / 7 + 1) + "周",
                        Collectors.summingDouble(SalesRecord::amount)))
                .entrySet()
                .stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .map(e -> e.getKey() + ": " + String.format("%.2f 万", e.getValue()))
                .collect(Collectors.joining("\n", "【" + period + " 销售趋势】\n", ""));
    }

    private List<SalesRecord> generateMockData(String period) {
        LocalDate start = "上月".equals(period)
                ? LocalDate.now().minusMonths(1).withDayOfMonth(1)
                : LocalDate.now().withDayOfMonth(1);
        Random random = new Random(start.toEpochDay());
        List<SalesRecord> records = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            LocalDate date = start.plusDays(i);
            if (date.getMonthValue() != start.getMonthValue()) break;
            if (date.getDayOfWeek().getValue() > 5) continue;
            for (int j = 0; j < 4; j++) {
                String region = REGIONS.get(random.nextInt(REGIONS.size()));
                String product = PRODUCTS.get(random.nextInt(PRODUCTS.size()));
                records.add(new SalesRecord(region, product, "销售" + (random.nextInt(8) + 1),
                        Math.round((5 + random.nextDouble() * 120) * 100) / 100.0, date));
            }
        }
        return records;
    }

    private static int normalizeLimit(Integer limit) {
        return limit == null || limit <= 0 ? 10 : Math.min(limit, 50);
    }

    private static String blankToDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record SalesRecord(String region, String product, String salesPerson, double amount, LocalDate date) {
    }

}
