package cis5550.search;

import static cis5550.search.Utils.parseQuery;
import static cis5550.search.Utils.stem;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDDImpl;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Job {
  private static final Logger logger = Logger.getLogger(Job.class);

  private static final boolean DEBUG = true;

  private static final int CACHE_LIFETIME_MINS = 15;
  private static final double a = 0.4;

  private static final ScheduledThreadPoolExecutor cleanupThreads = new ScheduledThreadPoolExecutor(
    1);

  static {
    cleanupThreads.setMaximumPoolSize(4);
  }

  public static void run(FlameContext ctx0, String[] args) throws Exception {
    if (args.length < 1) {
      ctx0.output("missing query argument");
      return;
    }
    String query = args[0];
    List<String> tokenizedQuery = parseQuery(query).sorted().toList();
    String queryKey = Hasher.hash(tokenizedQuery.toString());

    FlameContextImpl ctx = (FlameContextImpl) ctx0;
    KVSClient kvsMaster = ctx.getKVS();
    if (!kvsMaster.existsRow("cached", queryKey)) {
      FlameRDD queryTokens = ctx.parallelize(tokenizedQuery);
      FlamePairRDD queryPagesPairs = queryTokens.flatMapToPair(
        q -> () -> getIndex(q, ctx.getKVS()).flatMap(
          l -> Arrays.stream(q.replaceAll("^\"|\"$", " ").split("\\s+"))
            .map(w -> new FlamePair(w, l.substring(0, l.lastIndexOf(':'))))).iterator());
      queryTokens.drop();
      FlamePairRDD tfIdf = queryPagesPairs.flatMapToPair(p -> {
        KVSClient kvs = ctx.getKVS();
        Row tf = kvs.getRow("TF", Hasher.hash(p._2()));
        int maxTf = tf.columns().stream().filter(Predicate.not("__url"::equals)).map(tf::get)
          .map(Integer::parseInt).max(Integer::compare).orElse(0); // should not be 0
        int tfTok = Integer.parseInt(Objects.requireNonNullElse(tf.get(p._1()), "0"));
        double idf = Double.parseDouble(new String(
          Objects.requireNonNullElse(kvs.get("IDF", p._1(), "IDF"),
            "0.0".getBytes(StandardCharsets.UTF_8))));
        return Collections.singleton(
          new FlamePair(p._2(), String.valueOf((a + (1 - a) * tfTok / maxTf) * idf)));
      });
      queryPagesPairs.drop();
      FlamePairRDDImpl summedTfIdf = (FlamePairRDDImpl) tfIdf.foldByKey("0.0",
        (s1, s2) -> String.valueOf(Double.parseDouble(s1) + Double.parseDouble(s2)));
      tfIdf.drop();
      List<String> results = summedTfIdf.stream()
        .sorted(Comparator.comparing(p -> Double.parseDouble(((FlamePair) p)._2())).reversed())
        .map(FlamePair::_1).toList();
      summedTfIdf.drop();
      String paddingFormatString = "%0" + String.valueOf(results.size()).length() + "d";
      IntStream.range(0, results.size()).forEach(i -> {
        try {
          kvsMaster.put(queryKey, paddingFormatString.formatted(i), "url",
            results.get(i));
        } catch (IOException ignored) {
        }
      });

      cleanupThreads.schedule(() -> {
        try {
          refreshCache(ctx);
        } catch (Exception e) {
          Logger.getLogger(Job.class).error(e.getMessage(), e);
        }
      }, CACHE_LIFETIME_MINS, TimeUnit.MINUTES);
    }
    kvsMaster.put("cached", queryKey, "lastAccessed", String.valueOf(System.currentTimeMillis()));
  }

  private static void refreshCache(FlameContext ctx) throws Exception {
    KVSClient kvsClient = ctx.getKVS();
    kvsClient.rename("cached", "oldCache");
    kvsClient.scan("cached")
      .forEachRemaining(ent -> {
        try {
          if (System.currentTimeMillis() - Long.parseLong(ent.get("lastAccessed"))
              > CACHE_LIFETIME_MINS * 1000) {
            ctx.getKVS().delete(ent.key());
          } else {
            kvsClient.putRow("cached", ent);
          }
        } catch (IOException e) {
          logger.error(e.getMessage(), e);
        }
      });
    kvsClient.delete("oldCache");
  }

  private static String getPage(String url, KVSClient kvsClient) {
    try {
      return new String(kvsClient.get("crawl", Hasher.hash(url), "page"), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "";
    }
  }

  private static Stream<String> getIndex(String keyword, KVSClient kvsClient) {
    if (keyword.matches("^\".*\"$")) {
      String kw = keyword.replaceAll("^\"|\"$", "");
      Pattern kwPatttern = Pattern.compile(Pattern.quote(kw), Pattern.CASE_INSENSITIVE);
      System.out.println(kw);
      return Arrays.stream(kw.split("\\s+")).map(String::translateEscapes)
        .flatMap(w -> getIndex(w, kvsClient)).filter(
          url -> kwPatttern.matcher(getPage(url.replaceAll(":(?:\\d*\\s*)*$", ""), kvsClient))
            .find()).distinct();
    }
    try {
      Row entry = kvsClient.getRow("index", stem(keyword));
      if (entry != null) {
        return entry.columns().stream().map(entry::get).flatMap(e -> Arrays.stream(e.split(",")));
      } else {
        return Stream.empty();
      }
    } catch (IOException e) {
      return Stream.empty();
    }
  }
}
