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

package com.nageoffer.ai.ragent.rag.core.rewrite;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.ai.ragent.rag.dao.entity.QueryTermMappingDO;
import com.nageoffer.ai.ragent.rag.dao.mapper.QueryTermMappingMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class QueryTermMappingService {

    private final QueryTermMappingMapper mappingMapper;
    private final QueryTermMappingCacheManager cacheManager;

    /**
     * 对用户问题做术语归一化
     */
    public String normalize(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        // 从缓存加载映射规则
        List<QueryTermMappingDO> mappings = loadMappings();
        if (mappings.isEmpty()) {
            return text;
        }

        String result = text;
        for (QueryTermMappingDO mapping : mappings) {
            result = QueryTermMappingUtil.applyMapping(result, mapping.getSourceTerm(), mapping.getTargetTerm());
        }

        if (!Objects.equals(text, result)) {
            log.info("查询归一化：original='{}', normalized='{}'", text, result);
        }
        return result;
    }

    /**
     * 加载映射规则：优先从 Redis 缓存读取，缓存未命中则从数据库加载并回填缓存
     */
    private List<QueryTermMappingDO> loadMappings() {
        // 从 Redis 缓存读取
        List<QueryTermMappingDO> cached = cacheManager.getMappingsFromCache();
        if (CollUtil.isNotEmpty(cached)) {
            return filterApplicableMappings(cached);
        }

        // 缓存未命中，从数据库加载
        List<QueryTermMappingDO> dbList = mappingMapper.selectList(
                Wrappers.lambdaQuery(QueryTermMappingDO.class)
                        .eq(QueryTermMappingDO::getEnabled, 1)
        );
        List<QueryTermMappingDO> applicable = new ArrayList<>(filterApplicableMappings(dbList));
        sortMappings(applicable);

        // 回填 Redis 缓存
        cacheManager.saveMappingsToCache(applicable);
        log.info("术语映射规则从数据库加载完成，共 {} 条规则", applicable.size());
        return applicable;
    }

    /**
     * 仅保留当前运行时支持的规则：已启用、精确匹配、源/目标词非空。
     */
    private List<QueryTermMappingDO> filterApplicableMappings(List<QueryTermMappingDO> mappings) {
        if (CollUtil.isEmpty(mappings)) {
            return List.of();
        }
        return mappings.stream()
                .filter(m -> m.getEnabled() != null && m.getEnabled() == 1)
                .filter(m -> m.getMatchType() == null || m.getMatchType() == 1)
                .filter(m -> StrUtil.isNotBlank(m.getSourceTerm()) && StrUtil.isNotBlank(m.getTargetTerm()))
                .toList();
    }

    private void sortMappings(List<QueryTermMappingDO> mappings) {
        mappings.sort(Comparator
                .comparing(QueryTermMappingDO::getPriority, Comparator.nullsLast(Integer::compareTo)).reversed()
                .thenComparing(m -> m.getSourceTerm() == null ? 0 : m.getSourceTerm().length(), Comparator.reverseOrder())
        );
    }
}
