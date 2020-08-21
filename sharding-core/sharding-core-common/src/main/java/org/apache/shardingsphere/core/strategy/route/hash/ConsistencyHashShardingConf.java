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
