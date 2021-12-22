package org.apache.rocketmq.filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Filter factory: support other filter to register.
 */
public class FilterFactory {

    public static final FilterFactory INSTANCE = new FilterFactory();

    /**
     * key:org.apache.rocketmq.filter.FilterSpi#ofType()
     * value:过滤实例
     */
    protected static final Map<String, FilterSpi> FILTER_SPI_HOLDER = new HashMap<String, FilterSpi>(4);

    static {
        FilterFactory.INSTANCE.register(new SqlFilter());
    }

    /**
     * Register a filter.
     * <br>
     * Note:
     * <li>1. Filter registered will be used in broker server, so take care of it's reliability and performance.</li>
     */
    public void register(FilterSpi filterSpi) {
        if (FILTER_SPI_HOLDER.containsKey(filterSpi.ofType())) {
            throw new IllegalArgumentException(String.format("Filter spi type(%s) already exist!", filterSpi.ofType()));
        }

        FILTER_SPI_HOLDER.put(filterSpi.ofType(), filterSpi);
    }

    /**
     * Un register a filter.
     */
    public FilterSpi unRegister(String type) {
        return FILTER_SPI_HOLDER.remove(type);
    }

    /**
     * Get a filter registered, null if none exist.
     */
    public FilterSpi get(String type) {
        return FILTER_SPI_HOLDER.get(type);
    }

}
