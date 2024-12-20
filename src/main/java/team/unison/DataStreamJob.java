package team.unison;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import team.unison.model.AggregatedOutputEntry;
import team.unison.model.CsvDataEntry;

import java.io.File;
import java.io.FileReader;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        log.info("Read source file {}", args[0]);
         final File sourceFile = new File(args[0]);
        final int ratePerSecond = args.length > 1 ? Integer.parseInt(args[1]) : 1000;

        CsvToBean<CsvDataEntry> csv = new CsvToBeanBuilder<CsvDataEntry>(new FileReader(sourceFile))
                .withType(CsvDataEntry.class)
                .build();
        // load all data to memory, just for this demo
        List<CsvDataEntry> data = csv.parse();

        log.info("Create data generator source from csv file");
        // this is Flink's embedded data generator for testing purpose, can produce records with specified rate
        DataGeneratorSource<CsvDataEntry> source = new DataGeneratorSource<>(
                idx -> data.get(idx.intValue()),
                data.size(),
                RateLimiterStrategy.perSecond(ratePerSecond),
                TypeInformation.of(CsvDataEntry.class));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set to 1 to run task on single task manager to write result to single file
        env.setParallelism(1);
        DataStream<CsvDataEntry> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "data-source");

        inputStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<CsvDataEntry>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> System.currentTimeMillis()))
                .filter(entry -> entry.getLivingArea() != null && entry.getPrice() != null)
                .filter(entry -> entry.getLivingArea() >= 20 || entry.getPrice() >= 50000)
                // group entries by type and district
                // do not try to convert to lambda removing type information
                // (see details in https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/java_lambdas/)
                .keyBy(new KeySelector<CsvDataEntry, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(CsvDataEntry csvDataEntry) {
                        return Tuple2.of(csvDataEntry.getDistrict(), csvDataEntry.getType());
                    }
                })
                // create grouping tumbling window to process each group separately
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
                // run aggregation over window for each group
                .aggregate(new AverageAggregate(), new AverageProcess())
                .sinkTo(FileSink.forRowFormat(
                        new Path(sourceFile.getParent()),
                        new SimpleStringEncoder<AggregatedOutputEntry>())
                            .withRollingPolicy(
                                    DefaultRollingPolicy.builder()
                                            .withMaxPartSize(MemorySize.ofMebiBytes(100))
                                            .withRolloverInterval(Duration.ofHours(1))
                                            .build())
                        .build());

        env.execute("Real Estate aggregator");
    }

    // this one get aggregated over the window result and produce final output item for particular group over the window
    private static class AverageProcess extends ProcessWindowFunction<AggregatedOutputEntry, AggregatedOutputEntry, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> keyDistrictType, ProcessWindowFunction<AggregatedOutputEntry, AggregatedOutputEntry, Tuple2<String, String>, TimeWindow>.Context context, Iterable<AggregatedOutputEntry> elements, Collector<AggregatedOutputEntry> out) throws Exception {
            ValueState<AggregatedOutputEntry> state = context.globalState().getState(new ValueStateDescriptor<>("aggregatedOffers", AggregatedOutputEntry.class));
            // get stored object with aggregated sums
            AggregatedOutputEntry globalAggregation = Optional.ofNullable(state.value()).orElse(new AggregatedOutputEntry());
            // get current window object sums
            AggregatedOutputEntry currentAggregation = elements.iterator().next();
            // create new object to update global state
            globalAggregation = globalAggregation.sum(currentAggregation);
            state.update(globalAggregation);
            // create current result to collect
            AggregatedOutputEntry currentResult = globalAggregation.getAverage();
            currentResult.setDistrict(keyDistrictType.f0);
            currentResult.setType(keyDistrictType.f1);

            out.collect(currentResult);
        }
    }

    // this one sums required fields values from the incoming stream over the window and returns object with summed values when window ends
    private static class AverageAggregate implements AggregateFunction<CsvDataEntry, Tuple2<AggregatedOutputEntry, Long>, AggregatedOutputEntry> {

        @Override
        public Tuple2<AggregatedOutputEntry, Long> createAccumulator() {
            return Tuple2.of(new AggregatedOutputEntry(), 0L);
        }

        @Override
        public Tuple2<AggregatedOutputEntry, Long> add(CsvDataEntry entry, Tuple2<AggregatedOutputEntry, Long> accumulator) {
            accumulator.f0.setAveragePrice(accumulator.f0.getAveragePrice() + entry.getPrice());
            accumulator.f0.setAverageLivingArea(accumulator.f0.getAverageLivingArea() + entry.getLivingArea());
            accumulator.f0.setAveragePricePerSqm(accumulator.f0.getAveragePricePerSqm() + entry.getPrice() / entry.getLivingArea());
            accumulator.f1++;
            return accumulator;
        }

        @Override
        public AggregatedOutputEntry getResult(Tuple2<AggregatedOutputEntry, Long> accumulator) {
            accumulator.f0.setTotalAmount(accumulator.f1.intValue());
            return accumulator.f0;
        }

        @Override
        public Tuple2<AggregatedOutputEntry, Long> merge(Tuple2<AggregatedOutputEntry, Long> first, Tuple2<AggregatedOutputEntry, Long> second) {
            first.f0 = first.f0.sum(second.f0);
            return Tuple2.of(first.f0, first.f1 + second.f1);
        }

    }
}
