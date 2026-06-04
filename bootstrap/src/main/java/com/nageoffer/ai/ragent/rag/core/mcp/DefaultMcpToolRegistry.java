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

package com.nageoffer.ai.ragent.rag.core.mcp;

import cn.hutool.core.util.StrUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.mcp.SyncMcpToolCallbackProvider;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Default MCP tool registry backed by Spring AI MCP tool callbacks.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DefaultMcpToolRegistry implements McpToolRegistry {

    private final Map<String, ToolCallback> toolCallbackMap = new HashMap<>();

    private final ObjectProvider<SyncMcpToolCallbackProvider> toolCallbackProvider;

    private volatile boolean initialized = false;

    private void initializeIfNecessary() {
        if (initialized) {
            return;
        }
        synchronized (this) {
            if (initialized) {
                return;
            }
            try {
                SyncMcpToolCallbackProvider provider = toolCallbackProvider.getIfAvailable();
                if (provider == null) {
                    log.warn("Spring AI MCP tool callback provider is not available");
                    return;
                }
                for (ToolCallback toolCallback : provider.getToolCallbacks()) {
                    register(toolCallback);
                }
                initialized = true;
                log.info("MCP tools registered from Spring AI, total={}", toolCallbackMap.size());
            } catch (Exception e) {
                log.warn("MCP tools are not available now, skip registration. MCP server may be down. error={}",
                        e.getMessage());
            }
        }
    }

    private void register(ToolCallback toolCallback) {
        if (toolCallback == null || toolCallback.getToolDefinition() == null) {
            log.warn("Skip empty MCP tool callback");
            return;
        }

        String toolId = toolCallback.getToolDefinition().name();
        if (StrUtil.isBlank(toolId)) {
            log.warn("Skip MCP tool with blank toolId");
            return;
        }

        ToolCallback existing = toolCallbackMap.put(toolId, toolCallback);
        if (existing != null) {
            log.warn("MCP tool {} already exists and has been overwritten", toolId);
        } else {
            log.info("MCP tool registered, toolId={}", toolId);
        }
    }

    @Override
    public Optional<ToolCallback> getToolCallback(String toolId) {
        if (StrUtil.isBlank(toolId)) {
            return Optional.empty();
        }
        initializeIfNecessary();
        return Optional.ofNullable(toolCallbackMap.get(toolId));
    }
}
