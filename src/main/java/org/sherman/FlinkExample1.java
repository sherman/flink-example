package org.sherman;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Denis Gabaydulin
 * @since 08/07/2016
 */
public class FlinkExample1 {


    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> events = env.readTextFile("file:///home/sherman/prod-stat/statistics.2016-05-25.log");

        events.map(
                new MapFunction<String, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> map(String value) throws Exception {
                        return mapper.readValue(value, new TypeReference<Map<String, Object>>() {
                        });
                    }
                }
        ).map(
                new MapFunction<Map<String, Object>, Long>() {
                    @Override
                    public Long map(Map<String, Object> value) throws Exception {
                        return Optional.ofNullable(value.get("userId")).map(val -> (long) val).orElse(null);
                    }
                }
        )
                .filter(Objects::nonNull)
                .print();

        env.execute();
    }

    public static class Event implements Serializable {
    }
}
