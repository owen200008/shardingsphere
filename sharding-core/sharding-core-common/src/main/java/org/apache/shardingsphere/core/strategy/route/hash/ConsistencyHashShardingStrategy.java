package org.apache.shardingsphere.core.strategy.route.hash;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.apache.shardingsphere.api.config.sharding.strategy.ConsistencyHashShardingStrategyConfiguration;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.underlying.common.config.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * ConsistencyHash sharding strategy.
 */
public final class ConsistencyHashShardingStrategy implements ShardingStrategy {

    private final String shardingColumn;

    private final Collection<String> result;

    private final ConsistencyHashShardingConf consistencyHashShardingConf;

    public ConsistencyHashShardingStrategy(final ConsistencyHashShardingStrategyConfiguration inlineShardingStrategyConfig) {
        Preconditions.checkNotNull(inlineShardingStrategyConfig.getShardingColumn(), "Sharding column cannot be null.");
        Preconditions.checkNotNull(inlineShardingStrategyConfig.getJson(), "Sharding json cannot be null.");
        shardingColumn = inlineShardingStrategyConfig.getShardingColumn();
        consistencyHashShardingConf = JSON.parseObject(inlineShardingStrategyConfig.getJson(), ConsistencyHashShardingConf.class);
        consistencyHashShardingConf.init();

        result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.add(shardingColumn);
    }

    @Override
    public Collection<String> getShardingColumns() {
        return result;
    }

    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final Collection<RouteValue> shardingValues, final ConfigurationProperties properties) {
        RouteValue shardingValue = shardingValues.iterator().next();

        Preconditions.checkState(shardingValue instanceof ListRouteValue, "ConsistencyHash strategy cannot support this type sharding:" + shardingValue.toString());
        return doSharding((ListRouteValue) shardingValue);
    }

    private Collection<String> doSharding(final ListRouteValue shardingValue) {
        Collection<String> result = new LinkedList<>();
        for (PreciseShardingValue<?> each : transferToPreciseShardingValues(shardingValue)) {
            result.add(execute(each));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<PreciseShardingValue> transferToPreciseShardingValues(final ListRouteValue<?> shardingValue) {
        List<PreciseShardingValue> result = new ArrayList<>(shardingValue.getValues().size());
        for (Comparable<?> each : shardingValue.getValues()) {
            result.add(new PreciseShardingValue(shardingValue.getTableName(), shardingValue.getColumnName(), each));
        }
        return result;
    }

    private String execute(final PreciseShardingValue shardingValue) {
        return consistencyHashShardingConf.getValue(shardingValue.getValue().hashCode());
    }

}
