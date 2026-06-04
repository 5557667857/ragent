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
import java.util.List;
import java.util.Map;
import java.util.Random;

@Slf4j
@Component
public class WeatherMcpExecutor {

    private static final String TOOL_ID = "weather_query";

    private static final Map<String, double[]> CITY_COORDINATES = Map.ofEntries(
            Map.entry("北京", new double[]{39.9, 116.4}),
            Map.entry("上海", new double[]{31.2, 121.5}),
            Map.entry("广州", new double[]{23.1, 113.3}),
            Map.entry("深圳", new double[]{22.5, 114.1}),
            Map.entry("杭州", new double[]{30.3, 120.2}),
            Map.entry("成都", new double[]{30.6, 104.1}),
            Map.entry("武汉", new double[]{30.6, 114.3}),
            Map.entry("南京", new double[]{32.1, 118.8}),
            Map.entry("西安", new double[]{34.3, 108.9}),
            Map.entry("重庆", new double[]{29.6, 106.5})
    );

    private static final List<String> WEATHER_TYPES = List.of("晴", "多云", "阴", "小雨", "阵雨", "雷阵雨");

    @Tool(name = TOOL_ID, description = "查询城市天气信息，支持当前天气和未来多天天气预报。")
    public String queryWeather(
            @ToolParam(description = "城市名称，例如：北京、上海、广州。") String city,
            @ToolParam(required = false, description = "查询类型：current 表示当前天气，forecast 表示未来天气预报，默认 current。") String queryType,
            @ToolParam(required = false, description = "天气预报天数，默认 3 天，最多 7 天。") Integer days) {
        long startMs = System.currentTimeMillis();
        try {
            if (city == null || city.isBlank()) {
                return "请提供城市名称。";
            }
            if (!CITY_COORDINATES.containsKey(city)) {
                return "暂不支持查询该城市，当前支持：" + String.join("、", CITY_COORDINATES.keySet());
            }

            String normalizedQueryType = queryType == null || queryType.isBlank() ? "current" : queryType;
            int normalizedDays = days == null || days <= 0 ? 3 : Math.min(days, 7);
            String result = "forecast".equalsIgnoreCase(normalizedQueryType)
                    ? buildForecastResult(city, normalizedDays)
                    : buildCurrentResult(city);

            log.info("Spring AI MCP tool call completed, toolId={}, city={}, queryType={}, elapsed={}ms",
                    TOOL_ID, city, normalizedQueryType, System.currentTimeMillis() - startMs);
            return result;
        } catch (Exception e) {
            log.error("Spring AI MCP tool call failed, toolId={}, elapsed={}ms",
                    TOOL_ID, System.currentTimeMillis() - startMs, e);
            return "天气查询失败: " + e.getMessage();
        }
    }

    private String buildCurrentResult(String city) {
        WeatherData data = generateWeather(city, LocalDate.now());
        return """
                【%s 今日天气】
                天气: %s
                当前温度: %d°C
                最高温度: %d°C
                最低温度: %d°C
                湿度: %d%%
                风力: %s
                """.formatted(city, data.weatherType(), data.currentTemp(), data.highTemp(), data.lowTemp(),
                data.humidity(), data.windLevel()).trim();
    }

    private String buildForecastResult(String city, int days) {
        StringBuilder sb = new StringBuilder();
        sb.append("【").append(city).append(" 未来").append(days).append("天天气预报】\n");
        LocalDate today = LocalDate.now();
        for (int i = 0; i < days; i++) {
            LocalDate date = today.plusDays(i);
            WeatherData data = generateWeather(city, date);
            sb.append(date)
                    .append(": ")
                    .append(data.weatherType())
                    .append(", ")
                    .append(data.lowTemp())
                    .append("°C ~ ")
                    .append(data.highTemp())
                    .append("°C, 湿度 ")
                    .append(data.humidity())
                    .append("%, 风力 ")
                    .append(data.windLevel())
                    .append('\n');
        }
        return sb.toString().trim();
    }

    private WeatherData generateWeather(String city, LocalDate date) {
        double latitude = CITY_COORDINATES.get(city)[0];
        Random random = new Random(date.toEpochDay() * 31 + city.hashCode());
        int month = date.getMonthValue();
        double baseTemp = switch ((month % 12) / 3) {
            case 0 -> 5 - (latitude - 25) * 0.6;
            case 1 -> 18 - (latitude - 25) * 0.4;
            case 2 -> 31 - (latitude - 25) * 0.3;
            default -> 20 - (latitude - 25) * 0.4;
        };
        int highTemp = (int) Math.round(baseTemp + 3 + random.nextInt(5));
        int lowTemp = (int) Math.round(baseTemp - 4 - random.nextInt(4));
        int currentTemp = lowTemp + random.nextInt(Math.max(1, highTemp - lowTemp + 1));
        return new WeatherData(
                WEATHER_TYPES.get(random.nextInt(WEATHER_TYPES.size())),
                currentTemp,
                highTemp,
                lowTemp,
                35 + random.nextInt(55),
                (1 + random.nextInt(5)) + "级"
        );
    }

    private record WeatherData(String weatherType, int currentTemp, int highTemp, int lowTemp, int humidity,
                               String windLevel) {
    }
}
