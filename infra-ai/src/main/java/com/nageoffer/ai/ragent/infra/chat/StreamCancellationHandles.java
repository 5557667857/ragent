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

package com.nageoffer.ai.ragent.infra.chat;

import lombok.NoArgsConstructor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * StreamCancellationHandle 工具类
 * 用于构建常见的取消句柄，统一幂等取消语义
 */
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class StreamCancellationHandles {

    private static final StreamCancellationHandle NOOP = () -> {
    };

    public static StreamCancellationHandle noop() {
        return NOOP;
    }

    public static StreamCancellationHandle fromRunnable(Runnable cancelAction, AtomicBoolean cancelled) {
        return new RunnableCancellationHandle(cancelAction, cancelled);
    }

    private static final class RunnableCancellationHandle implements StreamCancellationHandle {

        private final Runnable cancelAction;
        private final AtomicBoolean cancelled;
        private final AtomicBoolean once = new AtomicBoolean(false);

        private RunnableCancellationHandle(Runnable cancelAction, AtomicBoolean cancelled) {
            this.cancelAction = cancelAction;
            this.cancelled = cancelled;
        }

        @Override
        public void cancel() {
            if (!once.compareAndSet(false, true)) {
                return;
            }
            if (cancelled != null) {
                cancelled.set(true);
            }
            if (cancelAction != null) {
                cancelAction.run();
            }
        }
    }
}
