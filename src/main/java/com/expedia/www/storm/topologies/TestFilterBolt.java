package com.expedia.www.storm.com.expedia.www.storm.topologies;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.expedia.www.commons.storm.bolts.AbstractBaseBolt;
import com.expedia.www.commons.storm.model.RollupCount;
import com.expedia.www.commons.storm.model.RollupItem;
import com.google.gson.Gson;

import java.util.Map;

/**
 * Created by rbagai on 4/17/14.
 */
public class TestFilterBolt extends BaseRichBolt
{
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
    }

    @Override
    public void execute(Tuple tuple)
    {
        String gsonString = tuple.getValue(0).toString();
        RollupCount<RollupItem> rollupObject = new Gson().fromJson(gsonString, RollupCount.class);

        if (rollupObject.key().id().equals("hotel.view.count")) {
            String hotelId = rollupObject.key().name();
            long count = rollupObject.count();
            long time = rollupObject.time();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {

    }
}
