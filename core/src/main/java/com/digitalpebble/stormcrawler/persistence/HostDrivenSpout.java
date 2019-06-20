/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
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

package com.digitalpebble.stormcrawler.persistence;

import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
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

    private Queue<HostInfo> hosts = new LinkedList<>();

    private boolean forceRefreshing = false;

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

		if (forceRefreshing) {
			needsRefreshing = true;
			forceRefreshing = false;
		}

		// needed and not already doing it
		if (needsRefreshing && !isGettingHosts.get()) {
			isGettingHosts.set(true);
			CompletableFuture<Queue<HostInfo>> futureHosts = refreshHostList();
			futureHosts.thenAccept(h -> {
				synchronized (hosts) {
					lastRefreshTimeHosts = Instant.now().toEpochMilli();
					hosts = h;
				}
			});
		}

		super.nextTuple();
	}

    protected void forceRefreshing() {
        forceRefreshing = true;
    }

    protected class HostInfo {
        private final String value;
        private Optional<Object> payload;

        HostInfo(String key) {
            this.value = key;
        }

        protected String getValue() {
            return this.value;
        }

        public Optional<Object> getPayload() {
            return payload;
        }

        public void setPayload(Object payload) {
            this.payload = Optional.of(payload);
        }

    }

    /** @return next HostInfo to be used by populateBuffer() **/
    protected HostInfo getNextHost() {
        HostInfo nextHost = hosts.poll();
        if (nextHost != null)
            hosts.offer(nextHost);
        return nextHost;
    }

    /**
     * Return a queue of HostInfo to use within populateBuffer(). The HostInfo
     * objects will be returned by getNextHost in the order in which they were
     * provided by this method.
     **/
    protected abstract CompletableFuture<Queue<HostInfo>> refreshHostList();

}