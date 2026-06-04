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

package com.nageoffer.ai.ragent.mcp.config;

import com.nageoffer.ai.ragent.mcp.tools.AmapCompanyAddressMcpExecutor;
import com.nageoffer.ai.ragent.mcp.tools.SalesMcpExecutor;
import com.nageoffer.ai.ragent.mcp.tools.TicketMcpExecutor;
import com.nageoffer.ai.ragent.mcp.tools.WeatherMcpExecutor;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes Spring AI @Tool methods as MCP tools through spring-ai-starter-mcp-server-webmvc.
 */
@Configuration
public class McpToolConfig {

    @Bean
    public ToolCallbackProvider mcpToolCallbackProvider(
            WeatherMcpExecutor weatherMcpExecutor,
            TicketMcpExecutor ticketMcpExecutor,
            SalesMcpExecutor salesMcpExecutor,
            AmapCompanyAddressMcpExecutor amapCompanyAddressMcpExecutor) {
        return MethodToolCallbackProvider.builder()
                .toolObjects(weatherMcpExecutor, ticketMcpExecutor, salesMcpExecutor, amapCompanyAddressMcpExecutor)
                .build();
    }
}
