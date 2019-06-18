package com.digitalpebble.stormcrawler.persistence;

import java.time.Instant;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Separates the handling of the hosts from the pulling of the URLs. A queue of
 * hosts to process is kept in memory and refreshed periodically.
 **/
@SuppressWarnings("serial")
public abstract class HostDrivenSpout extends AbstractQueryingSpout {

    /** how frequently the hosts should be refreshed, in seconds **/
    private static final String refreshHostsParamName = "spout.freq.refresh.hosts";

    private int frequencyRefreshHosts = 60;

    private AtomicBoolean isGettingHosts = new AtomicBoolean(false);

    private long lastRefreshTimeHosts = 0;

    private PriorityQueue<HostInfo> hosts;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void open(Map stormConf, TopologyContext context,
            SpoutOutputCollector collector) {
        frequencyRefreshHosts = ConfUtils.getInt(stormConf,
                refreshHostsParamName, frequencyRefreshHosts);
    }

    @Override
    public void nextTuple() {
        // check whether the list of hosts should be refreshed
        boolean needsRefreshing = true;
        if (lastRefreshTimeHosts != 0) {
            long difference = System.currentTimeMillis() - lastRefreshTimeHosts;
            if (difference < frequencyRefreshHosts * 1000) {
                needsRefreshing = false;
            }
        }
        // needed and not already doing it
        if (needsRefreshing && !isGettingHosts.get()) {
            isGettingHosts.set(true);
            CompletableFuture<PriorityQueue<HostInfo>> futureHosts = refreshHostList();
            futureHosts.thenAccept(h -> {
                synchronized (hosts) {
                    lastRefreshTimeHosts = Instant.now().toEpochMilli();
                    hosts = h;
                }
            });
        }

        super.nextTuple();
    }

    protected class HostInfo implements Comparable<HostInfo> {
        private final String value;
        private Instant lastQueried;

        HostInfo(String value) {
            this.value = value;
            setQueriedNow();
        }

        public int compareTo(HostInfo o) {
            // sort first by lastQueries then string value
            int timecomparison = this.lastQueried.compareTo(o.lastQueried);
            if (timecomparison != 0)
                return timecomparison;
            return value.compareTo(o.value);
        }

        protected String getValue() {
            return this.value;
        }

        protected void setQueriedNow() {
            lastQueried = Instant.now();
        }

    }

    /** @return PriorityQueue of HostInfo to be used by populateBuffer() **/
    protected PriorityQueue<HostInfo> getHosts() {
        return new PriorityQueue<HostInfo>(hosts);
    }

    /** Return a priorityqueue of HostInfo to use within populateBuffer() **/
    protected abstract CompletableFuture<PriorityQueue<HostInfo>> refreshHostList();

}