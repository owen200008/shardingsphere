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

package org.apache.shardingsphere.core.strategy.route.hash;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ConsistencyHashShardingConf {

    private Integer slot;

    private List<ConsistencyHashShardingConfItem> item;

    private Map<Integer, String> cache;

    /**
     * 获取hash一致性的数据库.
     * @param hashCode hash值
     * @return 返回数据库
     */
    public String getValue(final int hashCode) {
        return cache.get(Math.abs(hashCode) % slot);
    }

    /**
     * 初始化函数.
     */
    public void init() {
        this.cache = new HashMap();
        for (ConsistencyHashShardingConf.ConsistencyHashShardingConfItem consistencyHashShardingConfItem : item) {
            for (int i = consistencyHashShardingConfItem.getMin(); i <= consistencyHashShardingConfItem.getMax(); i++) {
                this.cache.put(i, consistencyHashShardingConfItem.getDb());
            }
        }
    }

    @Data
    public static class ConsistencyHashShardingConfItem {

        private String db;

        private Integer min;

        private Integer max;
    }
}
