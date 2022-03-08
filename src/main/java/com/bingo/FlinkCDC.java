package com.bingo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bingo.bean.DetailBean;
import com.bingo.bean.Dws_n_st_basic;
import com.bingo.bean.UvCount;
import com.bingo.func.CustomerDeserializationSchma;
import com.bingo.util.DimUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.codehaus.plexus.util.dag.DAG;
import scala.collection.mutable.HashSet;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;


public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        //1.获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(10);

//        //开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setAlignmentTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //程序重启保存检查点
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        env.setStateBackend(new FsStateBackend("file:///C:/Users/fth/Desktop/flinkCDC"));

        //2.通过flinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("172.30.12.101")
                .port(3306)
                .username("root")
                .password("i@SH021.bg")
                .databaseList("bgd")
                .tableList("bgd.data_detail")
                .deserializer(new CustomerDeserializationSchma())
                .startupOptions(StartupOptions.latest())
                .serverTimeZone("Asia/Shanghai")
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //转json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDetail = dataStreamSource.map(JSON::parseObject);

//        SingleOutputStreamOperator<JSONObject> filter = jsonObjDetail.filter(new RichFilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                String mmac = jsonObject.getJSONObject("after").getString("MMAC");
//                String mac = jsonObject.getString("MAC");
//                if (mmac.equals("14:6b:9c:f3:ed:27") /*&& mac.equals("bjyNgArNtkbNp7bNpkbNtjl=")*/) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });

        //实体类
        SingleOutputStreamOperator<DetailBean> detailBean = jsonObjDetail.map(jsonObject -> JSONObject.parseObject(jsonObject.getJSONObject("after").toString(), DetailBean.class));


        SingleOutputStreamOperator<Dws_n_st_basic> Dws_n_st_basicBean = detailBean.map(new DimUtil());

        SingleOutputStreamOperator<UvCount> apply = Dws_n_st_basicBean
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Dws_n_st_basic>(Time.milliseconds(0)) {
                    @Override
                    public long extractTimestamp(Dws_n_st_basic dws_n_st_basic) {
                        return dws_n_st_basic.getDtime();
                    }
                })
                .keyBy(new KeySelector<Dws_n_st_basic, String>() {
                    @Override
                    public String getKey(Dws_n_st_basic dws_n_st_basic) throws Exception {
                        return dws_n_st_basic.getProid();
                    }
                }).window(TumblingEventTimeWindows.of(/*Time.milliseconds(2000),*/Time.milliseconds(1000)))
                .apply(new WindowFunction<Dws_n_st_basic, UvCount, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Dws_n_st_basic> iterable, Collector<UvCount> collector) throws Exception {
                        HashSet<String> longHashSet = new HashSet<>();
                        Iterator<Dws_n_st_basic> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            longHashSet.add(iterator.next().getMac());
                        }
                        Long size = Long.valueOf(longHashSet.size());
                        collector.collect(new UvCount(s, size,timeWindow.getStart(),timeWindow.getEnd()));
                    }
                });

        apply.print();




//
//        //分组聚合
////        SingleOutputStreamOperator<String> detailBeanTuple2KeyedStream =
//        SingleOutputStreamOperator<UvCount> apply = detailBean
//                .keyBy(new KeySelector<DetailBean, String>() {
//                    @Override
//                    public String getKey(DetailBean detailBean) throws Exception {
//                        return detailBean.getMmac();
//                    }
//                }).window(TumblingProcessingTimeWindows.of( Time.milliseconds(2000),Time.milliseconds(1000)))
//                .apply(new WindowFunction<DetailBean, UvCount, String, TimeWindow>() {
//
//
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<DetailBean> iterable, Collector<UvCount> collector) throws Exception {
//                        HashSet<String> longHashSet = new HashSet<>();
//                        Iterator<DetailBean> iterator = iterable.iterator();
//                        while (iterator.hasNext()) {
//                            longHashSet.add(iterator.next().getMac());
//                        }
//                        String end = String.valueOf(timeWindow.getEnd());
//                        Long size = Long.valueOf(longHashSet.size());
//                        collector.collect(new UvCount(s, size));
//                    }
//                });


//                .aggregate(new AggregateFunction<DetailBean, Acc, String>() {
//                    @Override
//                    public Acc createAccumulator() {
//                        Acc acc = new Acc();
//                        acc.init();
//                        return acc;
//                    }
//
//                    @Override
//                    public Acc add(DetailBean detailBean, Acc acc) {
//                        acc.count.getAndAdd(1);
//                        return acc;
//                    }
//
//                    @Override
//                    public String getResult(Acc acc) {
//                        return acc.toString();
//                    }
//
//                    @Override
//                    public Acc merge(Acc acc, Acc acc1) {
//                        acc.count.addAndGet(acc1.count.get());
//                        return acc;
//                    }
//                }, new WindowFunction<String, String, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
//                        Iterator<String> iterator = iterable.iterator();
//                        while (iterator.hasNext()) {
//                            String next = iterator.next();
//                            collector.collect(s+" "+next);
//                        }
//                    }
//                });
//
//        detailBeanTuple2KeyedStream.print("-----");


//        apply.print();



        env.execute("FlinkCDC");

    }


    private static class Acc {
        AtomicLong count;

        public void init() {
            count = new AtomicLong(0);
        }

        public Acc() {
        }

        @Override
        public String toString() {
            return ""+count;
        }
    }

}

