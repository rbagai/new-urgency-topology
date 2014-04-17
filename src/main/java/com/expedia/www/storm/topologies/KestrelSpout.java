package com.expedia.www.storm.com.expedia.www.storm.topologies;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import com.expedia.www.commons.io.ConnectionManager;
import com.expedia.www.commons.io.ConnectionPool;
import com.expedia.www.commons.kestrel.memcache.KestrelMemcacheClient;
import com.expedia.www.commons.kestrel.thrift.Client;
import com.expedia.www.commons.kestrel.thrift.NodeSetAwarePooledConnectionManager;
import com.expedia.www.commons.kestrel.thrift.RawItem;
import com.expedia.www.commons.storm.kestrel.spouts.ConnectionPoolConfig;
import com.expedia.www.commons.zookeeper.kestrel.AWSNodeReader;
import com.expedia.www.commons.zookeeper.kestrel.NodeSetDiscoveryConfig;
import com.expedia.www.commons.zookeeper.kestrel.NodeType;
import com.expedia.www.commons.zookeeper.kestrel.Protocol;
import com.sun.org.apache.xpath.internal.NodeSet;
import org.apache.thrift.TException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Created by rbagai on 4/17/14.
 */
public class KestrelSpout extends BaseRichSpout
{
    private ConnectionManager<Client> kestrelConnectionManager;
    private Queue<RawItem> buffer;
    private SpoutOutputCollector collector;
    String queueName;

    private static String STREAM_ROLLUP_DATA = "ROLLUP_DATA";

    public KestrelSpout(String zookeepers, NodeType nodeType, Protocol protocol,
                        int zookeeperConnectionTimeout, int zookeeperConnectionRetryInterval,
                        String queueName,
                        ConnectionPoolConfig connectionPoolConfig)
    {
        NodeSetDiscoveryConfig nodeSetDiscoveryConfig = new NodeSetDiscoveryConfig(zookeepers, nodeType, protocol,
                zookeeperConnectionTimeout,zookeeperConnectionRetryInterval, new AWSNodeReader());

        kestrelConnectionManager = new NodeSetAwarePooledConnectionManager(nodeSetDiscoveryConfig,
                connectionPoolConfig.kestrelConnectionPoolTimeout(), connectionPoolConfig.kestrelConnectionTimeout(),
                connectionPoolConfig.maxConnectionsPerKestrelHost());

        buffer = new LinkedList<RawItem>();

        this.queueName = queueName;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if(buffer.isEmpty())
            readBuffer();

        collector.emit(STREAM_ROLLUP_DATA, new Values(buffer.poll()));
    }

    private void readBuffer() {
        try
        {
            List<RawItem> rawItems = kestrelConnectionManager.checkout().getResource().getItems(queueName, 100, 5, 0);
            for (RawItem rawItem : rawItems) {
                buffer.offer(rawItem);
            }
        }catch(TException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
