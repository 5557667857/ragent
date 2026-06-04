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

package com.nageoffer.ai.ragent.rag.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.ai.ragent.rag.controller.vo.ConversationMessageVO;
import com.nageoffer.ai.ragent.rag.dao.entity.ConversationDO;
import com.nageoffer.ai.ragent.rag.dao.entity.ConversationMessageDO;
import com.nageoffer.ai.ragent.rag.dao.entity.ConversationSummaryDO;
import com.nageoffer.ai.ragent.rag.dao.mapper.ConversationMapper;
import com.nageoffer.ai.ragent.rag.dao.mapper.ConversationMessageMapper;
import com.nageoffer.ai.ragent.rag.dao.mapper.ConversationSummaryMapper;
import com.nageoffer.ai.ragent.rag.enums.ConversationMessageOrder;
import com.nageoffer.ai.ragent.rag.service.MessageFeedbackService;
import com.nageoffer.ai.ragent.rag.service.ConversationMessageService;
import com.nageoffer.ai.ragent.rag.service.bo.ConversationMessageBO;
import com.nageoffer.ai.ragent.rag.service.bo.ConversationSummaryBO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ConversationMessageServiceImpl implements ConversationMessageService {

    private final ConversationMessageMapper conversationMessageMapper;
    private final ConversationSummaryMapper conversationSummaryMapper;
    private final ConversationMapper conversationMapper;
    private final MessageFeedbackService feedbackService;

    @Override
    public String addMessage(ConversationMessageBO conversationMessage) {
        ConversationMessageDO messageDO = BeanUtil.toBean(conversationMessage, ConversationMessageDO.class);
        conversationMessageMapper.insert(messageDO);
        return messageDO.getId();
    }

    @Override
    public List<ConversationMessageVO> listMessages(String conversationId, String userId, Integer limit, ConversationMessageOrder order) {
        // 1. 校验对话归属：确保这个 conversation 确实属于当前用户，防止越权访问
        ConversationDO conversation = conversationMapper.selectOne(
                Wrappers.lambdaQuery(ConversationDO.class)
                        .eq(ConversationDO::getConversationId, conversationId)
                        .eq(ConversationDO::getUserId, userId)
                        .eq(ConversationDO::getDeleted, 0)
        );
        // 2. 解析排序方向：null 或 ASC 都视为升序，只有明确传 DESC 才是降序
        boolean asc = order == null || order == ConversationMessageOrder.ASC;
        // 3. 查询消息记录：按 createTime 排序，limit 用于限制返回条数（null 时不限制）
        List<ConversationMessageDO> records = conversationMessageMapper.selectList(
                Wrappers.lambdaQuery(ConversationMessageDO.class)
                        .eq(ConversationMessageDO::getConversationId, conversationId)
                        .eq(ConversationMessageDO::getUserId, userId)
                        .eq(ConversationMessageDO::getDeleted, 0)
                        .orderBy(true, asc, ConversationMessageDO::getCreateTime)
                        .last(limit != null, "limit " + limit)
        );
        // 4. 空结果直接返回，避免后续处理空列表
        if (CollUtil.isEmpty(records)) {
            return List.of();
        }

        // 5. 降序查询的结果是「最新在前」的倒序，前端展示需要翻转回「最早在前」的时序
        if (!asc) {
            Collections.reverse(records);
        }

        // 6. 筛选出所有 assistant 消息的 ID，用于批量查询用户对每条回复的点赞/点踩状态
        List<String> assistantMessageIds = records.stream()
                .filter(record -> "assistant".equalsIgnoreCase(record.getRole()))
                .map(ConversationMessageDO::getId)
                .toList();
        // 7. 批量查询当前用户对这些 assistant 消息的投票记录（key=消息ID, value=投票值 1点赞/-1点踩/0无）
        Map<String, Integer> votesByMessageId = feedbackService.getUserVotes(userId, assistantMessageIds);

        // 8. DO → VO 转换：将数据库实体转为前端展示对象，同时把投票信息填充进去
        List<ConversationMessageVO> result = new ArrayList<>();
        for (ConversationMessageDO record : records) {
            ConversationMessageVO vo = ConversationMessageVO.builder()
                    .id(String.valueOf(record.getId()))
                    .conversationId(record.getConversationId())
                    .role(record.getRole())
                    .content(record.getContent())
                    .thinkingContent(record.getThinkingContent())
                    .thinkingDuration(record.getThinkingDuration())
                    .vote(votesByMessageId.get(record.getId()))  // 从批量查询结果中取出该消息的投票状态
                    .createTime(record.getCreateTime())
                    .build();
            result.add(vo);
        }

        return result;
    }

    @Override
    public void addMessageSummary(ConversationSummaryBO conversationSummary) {
        ConversationSummaryDO conversationSummaryDO = BeanUtil.toBean(conversationSummary, ConversationSummaryDO.class);
        conversationSummaryMapper.insert(conversationSummaryDO);
    }
}
