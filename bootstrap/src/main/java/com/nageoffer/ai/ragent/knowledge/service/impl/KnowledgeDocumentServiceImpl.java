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

package com.nageoffer.ai.ragent.knowledge.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.core.chunk.ChunkEmbeddingService;
import com.nageoffer.ai.ragent.core.chunk.ChunkingMode;
import com.nageoffer.ai.ragent.core.chunk.ChunkingOptions;
import com.nageoffer.ai.ragent.core.chunk.ChunkingStrategy;
import com.nageoffer.ai.ragent.core.chunk.ChunkingStrategyFactory;
import com.nageoffer.ai.ragent.core.chunk.VectorChunk;
import com.nageoffer.ai.ragent.core.parser.DocumentParserSelector;
import com.nageoffer.ai.ragent.core.parser.ParserType;
import com.nageoffer.ai.ragent.framework.context.UserContext;
import com.nageoffer.ai.ragent.framework.exception.ClientException;
import com.nageoffer.ai.ragent.framework.mq.producer.MessageQueueProducer;
import com.nageoffer.ai.ragent.ingestion.dao.entity.IngestionPipelineDO;
import com.nageoffer.ai.ragent.ingestion.dao.mapper.IngestionPipelineMapper;
import com.nageoffer.ai.ragent.ingestion.domain.context.IngestionContext;
import com.nageoffer.ai.ragent.ingestion.domain.pipeline.PipelineDefinition;
import com.nageoffer.ai.ragent.ingestion.engine.IngestionEngine;
import com.nageoffer.ai.ragent.ingestion.service.IngestionPipelineService;
import com.nageoffer.ai.ragent.knowledge.config.KnowledgeScheduleProperties;
import com.nageoffer.ai.ragent.knowledge.controller.request.KnowledgeChunkCreateRequest;
import com.nageoffer.ai.ragent.knowledge.controller.request.KnowledgeDocumentPageRequest;
import com.nageoffer.ai.ragent.knowledge.controller.request.KnowledgeDocumentUpdateRequest;
import com.nageoffer.ai.ragent.knowledge.controller.request.KnowledgeDocumentUploadRequest;
import com.nageoffer.ai.ragent.knowledge.controller.vo.KnowledgeChunkVO;
import com.nageoffer.ai.ragent.knowledge.controller.vo.KnowledgeDocumentChunkLogVO;
import com.nageoffer.ai.ragent.knowledge.controller.vo.KnowledgeDocumentSearchVO;
import com.nageoffer.ai.ragent.knowledge.controller.vo.KnowledgeDocumentVO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeBaseDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentChunkLogDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentChunkLogMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.nageoffer.ai.ragent.knowledge.enums.DocumentStatus;
import com.nageoffer.ai.ragent.knowledge.enums.ProcessMode;
import com.nageoffer.ai.ragent.knowledge.enums.SourceType;
import com.nageoffer.ai.ragent.knowledge.handler.RemoteFileFetcher;
import com.nageoffer.ai.ragent.knowledge.mq.event.KnowledgeDocumentChunkEvent;
import com.nageoffer.ai.ragent.knowledge.schedule.CronScheduleHelper;
import com.nageoffer.ai.ragent.knowledge.service.KnowledgeChunkService;
import com.nageoffer.ai.ragent.knowledge.service.KnowledgeDocumentScheduleService;
import com.nageoffer.ai.ragent.knowledge.service.KnowledgeDocumentService;
import com.nageoffer.ai.ragent.rag.core.vector.VectorSpaceId;
import com.nageoffer.ai.ragent.rag.core.vector.VectorStoreService;
import com.nageoffer.ai.ragent.rag.dto.StoredFileDTO;
import com.nageoffer.ai.ragent.rag.service.FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class KnowledgeDocumentServiceImpl implements KnowledgeDocumentService {

    private final KnowledgeBaseMapper knowledgeBaseMapper;
    private final KnowledgeDocumentMapper documentMapper;
    private final DocumentParserSelector parserSelector;
    private final ChunkingStrategyFactory chunkingStrategyFactory;
    private final FileStorageService fileStorageService;
    private final VectorStoreService vectorStoreService;
    private final KnowledgeChunkService knowledgeChunkService;
    private final ObjectMapper objectMapper;
    private final KnowledgeDocumentScheduleService scheduleService;
    private final IngestionPipelineService ingestionPipelineService;
    private final IngestionPipelineMapper ingestionPipelineMapper;
    private final IngestionEngine ingestionEngine;
    private final ChunkEmbeddingService chunkEmbeddingService;
    private final KnowledgeDocumentChunkLogMapper chunkLogMapper;
    private final TransactionOperations transactionOperations;
    private final MessageQueueProducer messageQueueProducer;
    private final KnowledgeScheduleProperties scheduleProperties;
    private final RemoteFileFetcher remoteFileFetcher;
    @Value("knowledge-document-chunk_topic${unique-name:}")
    private String chunkTopic;

    @Override
    public KnowledgeDocumentVO upload(String kbId, KnowledgeDocumentUploadRequest requestParam, MultipartFile file) {
        // 1. 先确认目标知识库存在。后续文件会存到该知识库对应的对象存储 Bucket / Collection 中。
        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(kbId);
        Assert.notNull(kbDO, () -> new ClientException("知识库不存在"));

        // 2. 解析文档来源类型，并校验来源参数。
        //    sourceType=file 时使用前端上传的 MultipartFile；
        //    sourceType=url 时使用 sourceLocation 远程拉取文件，并且可选配置定时刷新。
        SourceType sourceType = SourceType.normalize(requestParam.getSourceType());
        validateSourceAndSchedule(sourceType, requestParam);

        // 3. 把文件保存到对象存储，并拿到统一的文件元信息。
        //    对本地上传文件：直接上传 MultipartFile。
        //    对远程 URL：先下载远程文件，再上传到对象存储。
        StoredFileDTO stored = resolveStoredFile(kbDO.getCollectionName(), sourceType, requestParam.getSourceLocation(), file);

        // 4. 解析后续处理模式。
        //    chunk 模式：上传后等待触发普通分块；
        //    pipeline 模式：上传后等待触发指定 Pipeline 处理。
        ProcessModeConfig modeConfig = resolveProcessModeConfig(requestParam);

        // 5. 组装文档数据库记录。
        //    此时只登记文档基本信息和文件存储地址，真正的分块、Embedding、向量入库在 startChunk/executeChunk 阶段完成。
        KnowledgeDocumentDO documentDO = KnowledgeDocumentDO.builder()
                .kbId(kbId)
                .docName(stored.getOriginalFilename())
                .enabled(1)
                .chunkCount(0)
                .fileUrl(stored.getUrl())
                .fileType(stored.getDetectedType())
                .fileSize(stored.getSize())
                .status(DocumentStatus.PENDING.getCode())
                .sourceType(sourceType.getValue())
                .sourceLocation(SourceType.URL == sourceType ? StrUtil.trimToNull(requestParam.getSourceLocation()) : null)
                .scheduleEnabled(isScheduleEnabled(sourceType, requestParam) ? 1 : 0)
                .scheduleCron(isScheduleEnabled(sourceType, requestParam) ? StrUtil.trimToNull(requestParam.getScheduleCron()) : null)
                .processMode(modeConfig.processMode().getValue())
                .chunkStrategy(modeConfig.chunkingMode() != null ? modeConfig.chunkingMode().getValue() : null)
                .chunkConfig(modeConfig.chunkConfig())
                .pipelineId(modeConfig.pipelineId())
                .createdBy(UserContext.getUsername())
                .updatedBy(UserContext.getUsername())
                .build();

        // 6. 将文档记录插入数据库。初始状态为 PENDING，表示已上传但尚未完成分块处理。
        documentMapper.insert(documentDO);

        // 7. 返回前端展示需要的 VO。这里返回的是文档记录信息，不包含分块结果。
        return BeanUtil.toBean(documentDO, KnowledgeDocumentVO.class);
    }

    @Override
    public void startChunk(String docId) {
        KnowledgeDocumentChunkEvent event = KnowledgeDocumentChunkEvent.builder()
                .docId(docId)
                .operator(UserContext.getUsername())
                .build();

        messageQueueProducer.sendInTransaction(
                chunkTopic,
                docId,
                "文档分块",
                event,
                arg -> {
                    int updated = documentMapper.update(
                            new LambdaUpdateWrapper<KnowledgeDocumentDO>()
                                    .set(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
                                    .set(KnowledgeDocumentDO::getUpdatedBy, event.getOperator())
                                    .eq(KnowledgeDocumentDO::getId, docId)
                                    .ne(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
                    );
                    if (updated == 0) {
                        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
                        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
                        throw new ClientException("文档分块操作正在进行中，请稍后再试");
                    }
                    KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
                    event.setKbId(documentDO.getKbId());
                    scheduleService.upsertSchedule(documentDO);
                }
        );
    }

    @Override
    public void executeChunk(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        if (documentDO == null) {
            log.warn("文档不存在，跳过分块任务, docId={}", docId);
            return;
        }

        runChunkTask(documentDO);
    }

    /**
     * 执行一次完整的文档分块任务。
     *
     * <p>调用入口通常来自 MQ 消费：startChunk 只负责把文档标记为 RUNNING 并发送消息，
     * 真正的解析、分块、Embedding 和入库都在这里串起来执行。</p>
     */
    private void runChunkTask(KnowledgeDocumentDO documentDO) {
        String docId = documentDO.getId();
        ProcessMode processMode = ProcessMode.normalize(documentDO.getProcessMode());
        // 每次分块都会先记录一条运行日志，后续成功或失败时回写耗时、数量和错误信息。
        KnowledgeDocumentChunkLogDO chunkLog = KnowledgeDocumentChunkLogDO.builder()
                .docId(docId)
                .status(DocumentStatus.RUNNING.getCode())
                .processMode(processMode.getValue())
                .chunkStrategy(documentDO.getChunkStrategy())
                .pipelineId(documentDO.getPipelineId())
                .startTime(new Date())
                .build();
        chunkLogMapper.insert(chunkLog);

        long totalStartTime = System.currentTimeMillis();
        long extractDuration = 0;
        long chunkDuration = 0;
        long embedDuration = 0;
        long persistDuration = 0;

        try {
            List<VectorChunk> chunkResults;
            if (ProcessMode.PIPELINE == processMode) {
                // Pipeline 模式：把文件交给配置好的摄取流水线处理，解析/增强/分块由各节点完成。
                long start = System.currentTimeMillis();
                chunkResults = runPipelineProcess(documentDO);
                chunkDuration = System.currentTimeMillis() - start;
            } else {
                // Chunk 模式：使用内置流程，按“抽取文本 -> 分块 -> Embedding”顺序处理。
                ChunkProcessResult result = runChunkProcess(documentDO);
                extractDuration = result.extractDuration();
                chunkDuration = result.chunkDuration();
                embedDuration = result.embedDuration();
                chunkResults = result.chunks();
            }


            // 最后统一持久化：先清理旧数据，再写入新的 Chunk 记录和向量，避免重分块时残留旧结果。
            long persistStart = System.currentTimeMillis();
            String collectionName = resolveCollectionName(documentDO.getKbId());
            int savedCount = persistChunksAndVectorsAtomically(collectionName, docId, chunkResults);
            persistDuration = System.currentTimeMillis() - persistStart;

            long totalDuration = System.currentTimeMillis() - totalStartTime;
            updateChunkLog(chunkLog.getId(), DocumentStatus.SUCCESS.getCode(), savedCount,
                    extractDuration, chunkDuration, embedDuration, persistDuration, totalDuration, null);
        } catch (Exception e) {
            // 任意阶段失败都把文档置为 FAILED，并在分块日志里保留错误原因。
            log.error("文档分块任务执行失败：docId={}", docId, e);
            markChunkFailed(documentDO.getId());
            long totalDuration = System.currentTimeMillis() - totalStartTime;
            updateChunkLog(chunkLog.getId(), DocumentStatus.FAILED.getCode(), 0,
                    extractDuration, chunkDuration, embedDuration, persistDuration, totalDuration, e.getMessage());
        }
    }

    /**
     * 原子化保存分块结果。
     * <p>同一个事务里完成数据库 Chunk、向量库记录和文档状态更新，避免出现数据库成功但向量未更新、
     * 或向量已写入但文档状态仍是旧状态的中间态。</p>
     */
    private int persistChunksAndVectorsAtomically(String collectionName, String docId, List<VectorChunk> chunkResults) {
        List<KnowledgeChunkCreateRequest> chunks = chunkResults.stream()
                .map(vc -> {
                    KnowledgeChunkCreateRequest req = new KnowledgeChunkCreateRequest();
                    req.setChunkId(vc.getChunkId());
                    req.setIndex(vc.getIndex());
                    req.setContent(vc.getContent());
                    return req;
                })
                .collect(Collectors.toList());
        transactionOperations.executeWithoutResult(status -> {
            // 支持重复分块：先删除该文档旧的 Chunk 和向量，再写入本次结果。
            knowledgeChunkService.deleteByDocId(docId);
            knowledgeChunkService.batchCreate(docId, chunks);
            vectorStoreService.deleteDocumentVectors(collectionName, docId);
            vectorStoreService.indexDocumentChunks(collectionName, docId, chunkResults);
            // 只有 Chunk 和向量都写入成功后，才把文档状态更新为 SUCCESS。
            KnowledgeDocumentDO updateDocumentDO = KnowledgeDocumentDO.builder()
                    .id(docId)
                    .chunkCount(chunks.size())
                    .status(DocumentStatus.SUCCESS.getCode())
                    .updatedBy(UserContext.getUsername())
                    .build();
            documentMapper.updateById(updateDocumentDO);
        });
        return chunks.size();
    }

    /**
     * 回写本次分块任务的执行结果，供前端“分块详情/日志”查看。
     */
    private void updateChunkLog(String logId, String status, int chunkCount, long extractDuration,
                                long chunkDuration, long embedDuration, long persistDuration,
                                long totalDuration, String errorMessage) {
        KnowledgeDocumentChunkLogDO update = KnowledgeDocumentChunkLogDO.builder()
                .id(logId)
                .status(status)
                .chunkCount(chunkCount)
                .extractDuration(extractDuration)
                .chunkDuration(chunkDuration)
                .embedDuration(embedDuration)
                .persistDuration(persistDuration)
                .totalDuration(totalDuration)
                .errorMessage(errorMessage)
                .endTime(new Date())
                .build();
        chunkLogMapper.updateById(update);
    }

    /**
     * 使用分块策略处理文档，失败直接抛异常，由 runChunkTask 统一处理错误状态
     * 4 阶段中的前 3 阶段：Extract -> Chunk -> Embed
     */
    private ChunkProcessResult runChunkProcess(KnowledgeDocumentDO documentDO) {
        ChunkingMode chunkingMode = ChunkingMode.fromValue(documentDO.getChunkStrategy());
        ChunkingOptions config = buildChunkingOptions(chunkingMode, documentDO);

        // Extract：从对象存储读取原文件，并用 Tika 抽取纯文本。
        long extractStart = System.currentTimeMillis();
        try (InputStream is = fileStorageService.openStream(documentDO.getFileUrl())) {
            String text = parserSelector.select(ParserType.TIKA.getType()).extractText(is, documentDO.getDocName());
            long extractDuration = System.currentTimeMillis() - extractStart;

            // Chunk：根据文档配置选择 fixed_size 或 structure_aware 等分块策略。
            ChunkingStrategy chunkingStrategy = chunkingStrategyFactory.requireStrategy(chunkingMode);
            long chunkStart = System.currentTimeMillis();
            List<VectorChunk> chunks = chunkingStrategy.chunk(text, config);
            long chunkDuration = System.currentTimeMillis() - chunkStart;

            // Embed：为每个 Chunk 生成向量；向量写入动作在后面的持久化阶段完成。
            long embedStart = System.currentTimeMillis();
            chunkEmbeddingService.embed(chunks);
            long embedDuration = System.currentTimeMillis() - embedStart;

            return new ChunkProcessResult(chunks, extractDuration, chunkDuration, embedDuration);
        } catch (Exception e) {
            throw new RuntimeException("文档内容提取或分块失败", e);
        }
    }

    private record ChunkProcessResult(List<VectorChunk> chunks, long extractDuration, long chunkDuration,
                                      long embedDuration) {
    }

    private record ProcessModeConfig(ProcessMode processMode, ChunkingMode chunkingMode, String chunkConfig,
                                     String pipelineId) {
    }

    /**
     * 使用 Pipeline 处理文档，失败直接抛异常，由 runChunkTask 统一处理错误状态
     */
    private List<VectorChunk> runPipelineProcess(KnowledgeDocumentDO documentDO) {
        String docId = String.valueOf(documentDO.getId());
        String pipelineId = documentDO.getPipelineId();

        if (pipelineId == null) {
            throw new IllegalStateException("Pipeline模式下Pipeline ID为空：docId=" + docId);
        }

        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(documentDO.getKbId());

        // Pipeline 定义来自管理端配置，典型链路是 fetcher/parser/enhancer/chunker/indexer。
        PipelineDefinition pipelineDef = ingestionPipelineService.getDefinition(pipelineId);

        byte[] fileBytes;
        try (InputStream is = fileStorageService.openStream(documentDO.getFileUrl())) {
            fileBytes = is.readAllBytes();
        } catch (Exception e) {
            throw new RuntimeException("读取文件内容失败：docId=" + docId, e);
        }

        // 这里直接把文件字节放进上下文，所以 Pipeline 内通常会从 parser 节点开始消费 rawBytes。
        // skipIndexerWrite=true 表示流水线只产出 chunks，最终入库仍由 runChunkTask 统一处理。
        IngestionContext context = IngestionContext.builder()
                .taskId(docId)
                .pipelineId(pipelineId)
                .rawBytes(fileBytes)
                .mimeType(documentDO.getFileType())
                .vectorSpaceId(VectorSpaceId.builder()
                        .logicalName(kbDO.getCollectionName())
                        .build())
                .skipIndexerWrite(true)
                .build();

        // 摄取引擎会按 pipeline 的 nextNodeId 链式执行，产出的 chunks 写回 IngestionContext。
        IngestionContext result = ingestionEngine.execute(pipelineDef, context);

        if (result.getError() != null) {
            throw new RuntimeException("Pipeline执行失败：" + result.getError().getMessage(), result.getError());
        }

        List<VectorChunk> chunks = result.getChunks();
        if (chunks == null || chunks.isEmpty()) {
            log.warn("Pipeline执行完成但未产生分块：docId={}", docId);
            return List.of();
        }

        return chunks;
    }

    public void chunkDocument(KnowledgeDocumentDO documentDO) {
        if (documentDO == null) {
            return;
        }
        runChunkTask(documentDO);
    }

    private void markChunkFailed(String docId) {
        transactionOperations.executeWithoutResult(status -> {
            KnowledgeDocumentDO update = new KnowledgeDocumentDO();
            update.setId(docId);
            update.setStatus(DocumentStatus.FAILED.getCode());
            update.setUpdatedBy(UserContext.getUsername());
            documentMapper.updateById(update);
        });
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void delete(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));

        // 禁止在文档分块运行时删除
        if (DocumentStatus.RUNNING.getCode().equals(documentDO.getStatus())) {
            throw new ClientException("文档正在分块中，无法删除");
        }

        knowledgeChunkService.deleteByDocId(docId);
        scheduleService.deleteByDocId(docId);
        chunkLogMapper.delete(Wrappers.lambdaQuery(KnowledgeDocumentChunkLogDO.class)
                .eq(KnowledgeDocumentChunkLogDO::getDocId, docId));

        documentDO.setDeleted(1);
        documentDO.setUpdatedBy(UserContext.getUsername());
        documentMapper.deleteById(documentDO);

        String collectionName = resolveCollectionName(documentDO.getKbId());
        vectorStoreService.deleteDocumentVectors(collectionName, docId);
        deleteStoredFileQuietly(documentDO);
    }

    @Override
    public KnowledgeDocumentVO get(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
        return BeanUtil.toBean(documentDO, KnowledgeDocumentVO.class);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void update(String docId, KnowledgeDocumentUpdateRequest requestParam) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));

        // 禁止在文档分块运行时修改
        if (DocumentStatus.RUNNING.getCode().equals(documentDO.getStatus())) {
            throw new ClientException("文档正在分块中，无法修改");
        }

        String docName = requestParam == null ? null : requestParam.getDocName();
        if (!StringUtils.hasText(docName)) {
            throw new ClientException("文档名称不能为空");
        }

        LambdaUpdateWrapper<KnowledgeDocumentDO> updateWrapper = Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getId, documentDO.getId())
                .set(KnowledgeDocumentDO::getDocName, docName.trim())
                .set(KnowledgeDocumentDO::getUpdatedBy, UserContext.getUsername());

        // 如果传了 processMode，校验并更新处理配置
        if (StringUtils.hasText(requestParam.getProcessMode())) {
            ProcessMode processMode = ProcessMode.normalize(requestParam.getProcessMode());
            updateWrapper.set(KnowledgeDocumentDO::getProcessMode, processMode.getValue());

            if (ProcessMode.CHUNK == processMode) {
                ChunkingMode chunkingMode = ChunkingMode.fromValue(requestParam.getChunkStrategy());
                String chunkConfig = validateAndNormalizeChunkConfig(chunkingMode, requestParam.getChunkConfig());
                updateWrapper.set(KnowledgeDocumentDO::getChunkStrategy, chunkingMode.getValue());
                updateWrapper.setSql("chunk_config = CAST({0} AS jsonb)", chunkConfig);
                updateWrapper.set(KnowledgeDocumentDO::getPipelineId, null);
            } else {
                if (!StringUtils.hasText(requestParam.getPipelineId())) {
                    throw new ClientException("使用Pipeline模式时，必须指定Pipeline ID");
                }
                try {
                    ingestionPipelineService.get(requestParam.getPipelineId());
                } catch (Exception e) {
                    throw new ClientException("指定的Pipeline不存在: " + requestParam.getPipelineId());
                }
                updateWrapper.set(KnowledgeDocumentDO::getPipelineId, requestParam.getPipelineId());
                updateWrapper.set(KnowledgeDocumentDO::getChunkStrategy, null);
                updateWrapper.set(KnowledgeDocumentDO::getChunkConfig, null);
            }
        }

        // 处理定时调度相关字段（仅 URL 类型文档支持）
        boolean scheduleChanged = false;
        if (SourceType.URL.getValue().equalsIgnoreCase(documentDO.getSourceType())) {
            String newSourceLocation = requestParam.getSourceLocation();
            Integer newScheduleEnabled = requestParam.getScheduleEnabled();
            String newScheduleCron = requestParam.getScheduleCron();

            if (StringUtils.hasText(newSourceLocation)) {
                updateWrapper.set(KnowledgeDocumentDO::getSourceLocation, newSourceLocation.trim());
                scheduleChanged = true;
            }
            if (newScheduleEnabled != null) {
                updateWrapper.set(KnowledgeDocumentDO::getScheduleEnabled, newScheduleEnabled);
                scheduleChanged = true;
            }
            if (StringUtils.hasText(newScheduleCron)) {
                try {
                    CronScheduleHelper.nextRunTime(newScheduleCron, new Date());
                    // 验证 cron 周期不能太短（与 upsertSchedule 保持一致）
                    if (CronScheduleHelper.isIntervalLessThan(newScheduleCron, new Date(), 60)) {
                        throw new ClientException("定时周期不能小于 60 秒");
                    }
                } catch (IllegalArgumentException e) {
                    throw new ClientException("定时表达式不合法: " + e.getMessage());
                }
                updateWrapper.set(KnowledgeDocumentDO::getScheduleCron, newScheduleCron.trim());
                scheduleChanged = true;
            }

            // 验证：启用定时拉取时必须有 cron 和 sourceLocation
            if (scheduleChanged) {
                KnowledgeDocumentDO willBe = documentMapper.selectById(docId);
                Integer finalEnabled = newScheduleEnabled != null ? newScheduleEnabled : willBe.getScheduleEnabled();
                String finalCron = StringUtils.hasText(newScheduleCron) ? newScheduleCron.trim() : willBe.getScheduleCron();
                String finalLocation = StringUtils.hasText(newSourceLocation) ? newSourceLocation.trim() : willBe.getSourceLocation();

                if (finalEnabled != null && finalEnabled == 1) {
                    if (!StringUtils.hasText(finalCron)) {
                        throw new ClientException("启用定时拉取时必须设置定时表达式");
                    }
                    if (!StringUtils.hasText(finalLocation)) {
                        throw new ClientException("启用定时拉取时必须设置来源地址");
                    }
                }
            }
        }

        documentMapper.update(updateWrapper);

        if (scheduleChanged) {
            KnowledgeDocumentDO updated = documentMapper.selectById(docId);
            scheduleService.upsertSchedule(updated);
        }
    }

    @Override
    public IPage<KnowledgeDocumentVO> page(String kbId, KnowledgeDocumentPageRequest requestParam) {
        Page<KnowledgeDocumentDO> pageParam = new Page<>(requestParam.getCurrent(), requestParam.getSize());
        LambdaQueryWrapper<KnowledgeDocumentDO> queryWrapper = Wrappers.lambdaQuery(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getKbId, kbId)
                .eq(KnowledgeDocumentDO::getDeleted, 0)
                .like(requestParam.getKeyword() != null && !requestParam.getKeyword().isBlank(), KnowledgeDocumentDO::getDocName, requestParam.getKeyword())
                .eq(requestParam.getStatus() != null && !requestParam.getStatus().isBlank(), KnowledgeDocumentDO::getStatus, requestParam.getStatus())
                .orderByDesc(KnowledgeDocumentDO::getCreateTime);

        return documentMapper.selectPage(pageParam, queryWrapper)
                .convert(each -> BeanUtil.toBean(each, KnowledgeDocumentVO.class));
    }

    @Override
    public List<KnowledgeDocumentSearchVO> search(String keyword, int limit) {
        if (!StringUtils.hasText(keyword)) {
            return Collections.emptyList();
        }

        int size = Math.min(Math.max(limit, 1), 20);
        Page<KnowledgeDocumentDO> mpPage = new Page<>(1, size);
        LambdaQueryWrapper<KnowledgeDocumentDO> qw = new LambdaQueryWrapper<KnowledgeDocumentDO>()
                .eq(KnowledgeDocumentDO::getDeleted, 0)
                .like(KnowledgeDocumentDO::getDocName, keyword)
                .orderByDesc(KnowledgeDocumentDO::getUpdateTime);

        IPage<KnowledgeDocumentDO> result = documentMapper.selectPage(mpPage, qw);
        List<KnowledgeDocumentSearchVO> records = result.getRecords().stream()
                .map(each -> BeanUtil.toBean(each, KnowledgeDocumentSearchVO.class))
                .toList();
        if (records.isEmpty()) {
            return records;
        }

        Set<String> kbIds = new HashSet<>();
        for (KnowledgeDocumentSearchVO record : records) {
            if (record.getKbId() != null) {
                kbIds.add(record.getKbId());
            }
        }
        if (kbIds.isEmpty()) {
            return records;
        }

        List<KnowledgeBaseDO> bases = knowledgeBaseMapper.selectByIds(kbIds);
        Map<String, String> nameMap = new HashMap<>();
        if (bases != null) {
            for (KnowledgeBaseDO base : bases) {
                nameMap.put(base.getId(), base.getName());
            }
        }
        for (KnowledgeDocumentSearchVO record : records) {
            record.setKbName(nameMap.get(record.getKbId()));
        }
        return records;
    }

    @Override
    public void enable(String docId, boolean enabled) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));

        // 禁止在文档分块运行时修改
        if (DocumentStatus.RUNNING.getCode().equals(documentDO.getStatus())) {
            throw new ClientException("文档正在分块中，无法修改");
        }

        // 如果已经是目标状态，直接返回
        int targetEnabled = enabled ? 1 : 0;
        if (documentDO.getEnabled() != null && documentDO.getEnabled() == targetEnabled) {
            return;
        }

        // 提前查知识库，两个分支都需要，避免重复查询
        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(documentDO.getKbId());
        String collectionName = kbDO.getCollectionName();

        // 启用时：embed 耗时较长，在事务外提前执行，避免长事务占用连接
        List<VectorChunk> vectorChunks = null;
        if (enabled) {
            List<KnowledgeChunkVO> chunks = knowledgeChunkService.listByDocId(docId);
            vectorChunks = chunks.stream().map(each ->
                    VectorChunk.builder()
                            .chunkId(each.getId())
                            .content(each.getContent())
                            .index(each.getChunkIndex())
                            .build()
            ).toList();
            if (CollUtil.isEmpty(vectorChunks)) {
                log.warn("启用文档时未找到任何 Chunk，跳过向量重建，docId={}", docId);
                return;
            }
            chunkEmbeddingService.embed(vectorChunks);
        }

        final List<VectorChunk> finalVectorChunks = vectorChunks;
        transactionOperations.executeWithoutResult(status -> {
            documentDO.setEnabled(targetEnabled);
            documentDO.setUpdatedBy(UserContext.getUsername());
            documentMapper.updateById(documentDO);
            scheduleService.syncScheduleIfExists(documentDO);
            knowledgeChunkService.updateEnabledByDocId(docId, String.valueOf(kbDO.getId()), enabled);

            if (!enabled) {
                vectorStoreService.deleteDocumentVectors(collectionName, docId);
            } else {
                vectorStoreService.indexDocumentChunks(collectionName, docId, finalVectorChunks);
            }
        });
    }

    @Override
    public IPage<KnowledgeDocumentChunkLogVO> getChunkLogs(String docId, Page<KnowledgeDocumentChunkLogVO> page) {
        Page<KnowledgeDocumentChunkLogDO> mpPage = new Page<>(page.getCurrent(), page.getSize());
        LambdaQueryWrapper<KnowledgeDocumentChunkLogDO> qw = new LambdaQueryWrapper<KnowledgeDocumentChunkLogDO>()
                .eq(KnowledgeDocumentChunkLogDO::getDocId, docId)
                .orderByDesc(KnowledgeDocumentChunkLogDO::getCreateTime);

        IPage<KnowledgeDocumentChunkLogDO> result = chunkLogMapper.selectPage(mpPage, qw);

        List<KnowledgeDocumentChunkLogDO> records = result.getRecords();
        Map<String, String> pipelineNameMap = new HashMap<>();
        if (CollUtil.isNotEmpty(records)) {
            Set<String> pipelineIds = new HashSet<>();
            for (KnowledgeDocumentChunkLogDO record : records) {
                if (record.getPipelineId() != null) {
                    pipelineIds.add(record.getPipelineId());
                }
            }
            if (!pipelineIds.isEmpty()) {
                List<IngestionPipelineDO> pipelines = ingestionPipelineMapper.selectByIds(pipelineIds);
                if (CollUtil.isNotEmpty(pipelines)) {
                    for (IngestionPipelineDO pipeline : pipelines) {
                        pipelineNameMap.put(pipeline.getId(), pipeline.getName());
                    }
                }
            }
        }

        Page<KnowledgeDocumentChunkLogVO> voPage = new Page<>(result.getCurrent(), result.getSize(), result.getTotal());
        voPage.setRecords(records.stream().map(each -> {
            KnowledgeDocumentChunkLogVO vo = BeanUtil.toBean(each, KnowledgeDocumentChunkLogVO.class);
            if (each.getPipelineId() != null) {
                vo.setPipelineName(pipelineNameMap.get(each.getPipelineId()));
            }
            Long totalDuration = each.getTotalDuration();
            if (totalDuration != null) {
                long other = getOther(each, totalDuration);
                vo.setOtherDuration(Math.max(0, other));
            }
            return vo;
        }).toList());
        return voPage;
    }

    private static long getOther(KnowledgeDocumentChunkLogDO each, Long totalDuration) {
        String mode = each.getProcessMode();
        boolean pipelineMode = ProcessMode.PIPELINE.getValue().equalsIgnoreCase(mode);
        long extract = each.getExtractDuration() == null ? 0 : each.getExtractDuration();
        long chunk = each.getChunkDuration() == null ? 0 : each.getChunkDuration();
        long embed = each.getEmbedDuration() == null ? 0 : each.getEmbedDuration();
        long persist = each.getPersistDuration() == null ? 0 : each.getPersistDuration();
        return pipelineMode
                ? totalDuration - chunk - persist
                : totalDuration - extract - chunk - embed - persist;
    }

    private String resolveCollectionName(String kbId) {
        return knowledgeBaseMapper.selectById(kbId).getCollectionName();
    }

    private boolean isScheduleEnabled(SourceType sourceType, KnowledgeDocumentUploadRequest request) {
        return SourceType.URL == sourceType && Boolean.TRUE.equals(request.getScheduleEnabled());
    }

    private void validateSourceAndSchedule(SourceType sourceType, KnowledgeDocumentUploadRequest request) {
        String sourceLocation = StrUtil.trimToNull(request.getSourceLocation());
        if (SourceType.URL == sourceType && !StringUtils.hasText(sourceLocation)) {
            throw new ClientException("来源地址不能为空");
        }
        if (!isScheduleEnabled(sourceType, request)) {
            return;
        }
        String scheduleCron = StrUtil.trimToNull(request.getScheduleCron());
        if (!StringUtils.hasText(scheduleCron)) {
            throw new ClientException("定时表达式不能为空");
        }
        try {
            if (CronScheduleHelper.isIntervalLessThan(scheduleCron, new java.util.Date(), scheduleProperties.getMinIntervalSeconds())) {
                throw new ClientException("定时周期不能小于 " + scheduleProperties.getMinIntervalSeconds() + " 秒");
            }
        } catch (IllegalArgumentException e) {
            throw new ClientException("定时表达式不合法");
        }
    }

    private ProcessModeConfig resolveProcessModeConfig(KnowledgeDocumentUploadRequest request) {
        ProcessMode processMode = ProcessMode.normalize(request.getProcessMode());
        if (ProcessMode.CHUNK == processMode) {
            ChunkingMode chunkingMode = ChunkingMode.fromValue(request.getChunkStrategy());
            String chunkConfig = validateAndNormalizeChunkConfig(chunkingMode, request.getChunkConfig());
            return new ProcessModeConfig(processMode, chunkingMode, chunkConfig, null);
        } else {
            if (!StringUtils.hasText(request.getPipelineId())) {
                throw new ClientException("使用Pipeline模式时，必须指定Pipeline ID");
            }
            try {
                ingestionPipelineService.get(request.getPipelineId());
            } catch (Exception e) {
                throw new ClientException("指定的Pipeline不存在: " + request.getPipelineId());
            }
            return new ProcessModeConfig(processMode, null, null, request.getPipelineId());
        }
    }

    private StoredFileDTO resolveStoredFile(String bucketName, SourceType sourceType, String sourceLocation, MultipartFile file) {
        if (SourceType.FILE == sourceType) {
            Assert.notNull(file, () -> new ClientException("上传文件不能为空"));
            return fileStorageService.upload(bucketName, file);
        }
        return remoteFileFetcher.fetchAndStore(bucketName, sourceLocation);
    }

    private ChunkingOptions buildChunkingOptions(ChunkingMode mode, KnowledgeDocumentDO documentDO) {
        Map<String, Object> config = parseChunkConfig(documentDO.getChunkConfig());
        return mode.createOptions(config);
    }

    private String validateAndNormalizeChunkConfig(ChunkingMode mode, String chunkConfigJson) {
        if (!StringUtils.hasText(chunkConfigJson)) {
            return null;
        }
        if (mode == null) {
            mode = ChunkingMode.STRUCTURE_AWARE;
        }
        String json = chunkConfigJson.trim();
        Map<String, Object> config;
        try {
            config = objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new ClientException("分块参数JSON格式不合法");
        }
        for (String key : mode.getDefaultConfig().keySet()) {
            if (!config.containsKey(key)) {
                throw new ClientException("分块参数缺少必要字段: " + key);
            }
        }
        return json;
    }

    private Map<String, Object> parseChunkConfig(String json) {
        if (!StringUtils.hasText(json)) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception e) {
            log.warn("分块参数解析失败: {}", json, e);
            return Map.of();
        }
    }

    @Override
    public String preview(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("Document does not exist"));
        if (!"markdown".equalsIgnoreCase(documentDO.getFileType())
                && !"md".equalsIgnoreCase(documentDO.getFileType())) {
            throw new ClientException("Only markdown documents can be previewed");
        }
        try (InputStream inputStream = fileStorageService.openStream(documentDO.getFileUrl())) {
            return new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (ClientException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ClientException("Failed to read document content: " + ex.getMessage());
        }
    }

    private void deleteStoredFileQuietly(KnowledgeDocumentDO documentDO) {
        if (documentDO == null || !StringUtils.hasText(documentDO.getFileUrl())) {
            return;
        }
        try {
            fileStorageService.deleteByUrl(documentDO.getFileUrl());
        } catch (Exception e) {
            log.warn("删除文档存储文件失败, docId={}, fileUrl={}", documentDO.getId(), documentDO.getFileUrl(), e);
        }
    }
}
