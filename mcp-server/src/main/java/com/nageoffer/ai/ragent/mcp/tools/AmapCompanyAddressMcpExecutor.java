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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.mcp.config.AmapProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AmapCompanyAddressMcpExecutor {

    private static final String TOOL_ID = "amap_company_address_query";
    private static final String PLACE_TEXT_API = "https://restapi.amap.com/v3/place/text";

    private final AmapProperties amapProperties;
    private final ObjectMapper objectMapper;

    @Tool(name = TOOL_ID, description = "使用高德地图根据公司名称或机构名称查询公司地址、经纬度、城市、区县和地点类型。")
    public String queryCompanyAddress(
            @ToolParam(description = "公司名称或机构名称，例如：阿里巴巴西溪园区、腾讯滨海大厦。") String companyName,
            @ToolParam(required = false, description = "城市名称或城市编码，用于缩小查询范围，例如：杭州、深圳、北京。") String city,
            @ToolParam(required = false, description = "返回结果数量，默认 5，最多 20。") Integer limit) {
        long startMs = System.currentTimeMillis();
        try {
            if (!StringUtils.hasText(companyName)) {
                return "请提供公司名称或机构名称。";
            }
            if (!StringUtils.hasText(amapProperties.getApiKey())) {
                return "高德地图 API Key 未配置，请设置环境变量 AMAP_API_KEY 或配置 amap.api-key。";
            }

            int normalizedLimit = normalizeLimit(limit);
            URI uri = buildUri(companyName.trim(), city, normalizedLimit);
            HttpClient httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(amapProperties.getConnectTimeoutMs()))
                    .build();
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofMillis(amapProperties.getRequestTimeoutMs()))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return "高德地图地点查询失败，HTTP 状态码: " + response.statusCode();
            }

            String result = formatResponse(companyName.trim(), response.body(), normalizedLimit);
            log.info("Spring AI MCP tool call completed, toolId={}, companyName={}, city={}, elapsed={}ms",
                    TOOL_ID, companyName, city, System.currentTimeMillis() - startMs);
            return result;
        } catch (Exception e) {
            log.error("Spring AI MCP tool call failed, toolId={}, elapsed={}ms",
                    TOOL_ID, System.currentTimeMillis() - startMs, e);
            return "高德地图地点查询失败: " + e.getMessage();
        }
    }

    private URI buildUri(String companyName, String city, int limit) {
        List<String> params = new ArrayList<>();
        params.add("key=" + encode(amapProperties.getApiKey()));
        params.add("keywords=" + encode(companyName));
        params.add("offset=" + limit);
        params.add("page=1");
        params.add("extensions=base");
        params.add("output=json");
        if (StringUtils.hasText(city)) {
            params.add("city=" + encode(city.trim()));
            params.add("citylimit=false");
        }
        return URI.create(PLACE_TEXT_API + "?" + String.join("&", params));
    }

    private String formatResponse(String companyName, String body, int limit) throws Exception {
        JsonNode root = objectMapper.readTree(body);
        String status = text(root, "status");
        if (!"1".equals(status)) {
            String info = text(root, "info");
            String infoCode = text(root, "infocode");
            return "高德地图地点查询失败，错误信息: " + blankToDefault(info, "未知错误")
                    + "，错误码: " + blankToDefault(infoCode, "未知");
        }

        JsonNode pois = root.path("pois");
        if (!pois.isArray() || pois.isEmpty()) {
            return "未查询到与“" + companyName + "”匹配的公司或机构地点。";
        }

        int count = Math.min(limit, pois.size());
        StringBuilder sb = new StringBuilder();
        sb.append("【高德地图公司地址查询】\n");
        sb.append("查询关键词: ").append(companyName).append('\n');
        sb.append("匹配结果数: ").append(text(root, "count")).append('\n');
        sb.append("返回结果:\n");
        for (int i = 0; i < count; i++) {
            JsonNode poi = pois.get(i);
            sb.append(i + 1).append(". ")
                    .append(blankToDefault(text(poi, "name"), "未知名称")).append('\n');
            appendLine(sb, "地址", text(poi, "address"));
            appendLine(sb, "经纬度", text(poi, "location"));
            appendLine(sb, "省份", text(poi, "pname"));
            appendLine(sb, "城市", text(poi, "cityname"));
            appendLine(sb, "区县", text(poi, "adname"));
            appendLine(sb, "类型", text(poi, "type"));
            appendLine(sb, "POI ID", text(poi, "id"));
            if (i < count - 1) {
                sb.append('\n');
            }
        }
        return sb.toString().trim();
    }

    private void appendLine(StringBuilder sb, String label, String value) {
        if (StringUtils.hasText(value) && !"[]".equals(value)) {
            sb.append("   ").append(label).append(": ").append(value).append('\n');
        }
    }

    private int normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) {
            return 5;
        }
        return Math.min(limit, 20);
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() ? "" : value.asText("");
    }

    private String blankToDefault(String value, String defaultValue) {
        return StringUtils.hasText(value) ? value : defaultValue;
    }

    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
