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

package com.nageoffer.ai.ragent.rag.core.llm;

import com.nageoffer.ai.ragent.framework.convention.ChatMessage;
import com.nageoffer.ai.ragent.framework.convention.ChatRequest;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Small adapter between the project's simple chat DTOs and Spring AI 1.1.0.
 */
public final class SpringAiChatSupport {

    private SpringAiChatSupport() {
    }

    public static String chat(ChatModel chatModel, ChatRequest request) {
        return content(chatModel.call(toPrompt(request)));
    }

    public static Prompt toPrompt(ChatRequest request) {
        ChatRequest actual = request == null ? ChatRequest.builder().build() : request;
        return new Prompt(toMessages(actual.getMessages()), toOptions(actual));
    }

    public static String content(ChatResponse response) {
        if (response == null || response.getResult() == null || response.getResult().getOutput() == null) {
            return "";
        }
        String text = response.getResult().getOutput().getText();
        return text == null ? "" : text;
    }

    private static List<Message> toMessages(List<ChatMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return List.of();
        }
        List<Message> result = new ArrayList<>(messages.size());
        for (ChatMessage message : messages) {
            if (message == null) {
                continue;
            }
            String content = message.getContent() == null ? "" : message.getContent();
            result.add(switch (message.getRole()) {
                case SYSTEM -> new SystemMessage(content);
                case ASSISTANT -> new AssistantMessage(content);
                case USER -> new UserMessage(content);
            });
        }
        return result;
    }

    private static OpenAiChatOptions toOptions(ChatRequest request) {
        OpenAiChatOptions.Builder builder = OpenAiChatOptions.builder();
        if (request.getTemperature() != null) {
            builder.temperature(request.getTemperature());
        }
        if (request.getTopP() != null) {
            builder.topP(request.getTopP());
        }
        if (request.getMaxTokens() != null) {
            builder.maxTokens(request.getMaxTokens());
        }
        if (Boolean.TRUE.equals(request.getThinking())) {
            builder.extraBody(Map.of("enable_thinking", true));
        }
        return builder.build();
    }
}
