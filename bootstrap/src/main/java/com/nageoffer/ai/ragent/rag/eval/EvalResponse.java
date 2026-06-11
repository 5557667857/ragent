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

package com.nageoffer.ai.ragent.rag.eval;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Pure retrieval evidence returned to the evaluation runner.
 */
@Data
@Builder
public class EvalResponse {

    /**
     * Deduplicated business document IDs derived from document file names.
     */
    private List<String> retrievedDocIds;

    /**
     * Deduplicated chunk primary keys.
     */
    private List<String> retrievedChunkIds;

    /**
     * Retrieved chunk contents in the same order as retrievedChunkIds.
     */
    private List<String> retrievedContexts;

    /**
     * Business document ID for each retrieved context, preserving null values.
     */
    private List<String> retrievedContextDocIds;

    /**
     * MCP tool context produced during retrieval.
     */
    private String mcpContext;

    /**
     * Whether the retrieval used an MCP branch.
     */
    private boolean hasMcp;

    /**
     * Whether the retrieval returned knowledge-base evidence.
     */
    private boolean hasKb;

    /**
     * Rewritten and split sub-questions.
     */
    private List<String> subIntents;

    /**
     * Top intent leaf ID for each sub-question.
     */
    private List<String> intentLeafIds;

    /**
     * End-to-end retrieval latency in milliseconds.
     */
    private long latencyMs;
}
