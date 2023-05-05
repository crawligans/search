package cis5550.search;

import static cis5550.search.Utils.parseQuery;

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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Job {

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
        q -> () -> getIndex(q, ctx.getKVS()).map(l -> {
          int colIdx = l.lastIndexOf(':');
          return new FlamePair(q, l.substring(0, colIdx));
        }).iterator());
      FlamePairRDD tfIdf = queryPagesPairs.flatMapToPair(p -> {
        KVSClient kvs = ctx.getKVS();
        Row tf = kvs.getRow("TF", Hasher.hash(p._2()));
        int maxTf = tf.columns().stream().filter(Predicate.not("__url"::equals)).map(tf::get)
          .map(Integer::parseInt).max(Integer::compare).orElse(0);
        int tfTok = Integer.parseInt(tf.get(p._1()));
        double idf = Double.parseDouble(new String(kvs.get("IDF", p._1(), "IDF")));
        return Collections.singleton(
          new FlamePair(p._2(), String.valueOf((a + (1 - a) * tfTok / maxTf) * idf)));
      });
      FlamePairRDDImpl summedTfIdf = (FlamePairRDDImpl) tfIdf.foldByKey("0.0",
        (s1, s2) -> String.valueOf(Double.parseDouble(s1) + Double.parseDouble(s2)));
      List<String> results = summedTfIdf.stream()
        .sorted(Comparator.comparing(p -> Double.parseDouble(((FlamePair) p)._2())).reversed())
        .map(FlamePair::_1).toList();
      String paddingFormatString = "%0" + String.valueOf(results.size()).length() + "d";
      IntStream.range(0, results.size()).forEach(i -> {
        try {
          kvsMaster.put(queryKey, paddingFormatString.formatted(i), "url",
            results.get(i));
        } catch (IOException ignored) {
        }
      });

      kvsMaster.put("cached", queryKey, "lastAccessed", String.valueOf(System.currentTimeMillis()));
      cleanupThreads.schedule(() -> {
        try {
          refreshCache(ctx);
        } catch (Exception e) {
          Logger.getLogger(Job.class).error(e.getMessage(), e);
        }
      }, CACHE_LIFETIME_MINS, TimeUnit.MINUTES);
    }
  }

  private static void refreshCache(FlameContext ctx) throws Exception {
    FlamePairRDD cacheEntries = new FlamePairRDDImpl((FlameContextImpl) ctx, "cached");
    FlamePairRDD newCacheEntries = cacheEntries.flatMapToPair(ent -> {
      if (System.currentTimeMillis() - Long.parseLong(ent._2()) > CACHE_LIFETIME_MINS * 1000) {
        try {
          ctx.getKVS().delete(ent._1());
        } catch (IOException ignored) {
        }
        return Collections::emptyIterator;
      } else {
        return List.of(new FlamePair(ent._1(), String.valueOf(System.currentTimeMillis())));
      }
    });
    cacheEntries.drop();
    newCacheEntries.saveAsTable("cached");
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
      return Arrays.stream(kw.split("\\s")).map(String::translateEscapes)
        .flatMap(w -> getIndex(w, kvsClient)).filter(
          url -> Pattern.compile(Pattern.quote(kw), Pattern.CASE_INSENSITIVE)
            .matcher(getPage(url.replaceAll(":(?:\\d*\\s*)*$", ""), kvsClient)).find());
    }
    try {
      Row entry = kvsClient.getRow("index", keyword);
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
