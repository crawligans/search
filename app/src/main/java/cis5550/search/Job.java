package cis5550.search;

import static cis5550.search.Utils.parseQuery;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Job {

  private static final boolean DEBUG = true;

  private static final int CACHE_LIFETIME_MINS = 15;

  private static final ScheduledThreadPoolExecutor cleanupThreads = new ScheduledThreadPoolExecutor(
    1);

  static {
    cleanupThreads.setMaximumPoolSize(4);
  }

  public static void run(FlameContext ctx, String[] args) throws Exception {
    if (args.length < 1) {
      ctx.output("missing query argument");
      return;
    }
    String query = args[0];
    List<String> tokenizedQuery = parseQuery(query).sorted().toList();

    final int Q_LEN = tokenizedQuery.size();
    for(int i = 0; i < Q_LEN ; i++ ){
      tokenizedQuery.set(i, tokenizedQuery.get(i) + "=" + i);
    }
    String queryKey = URLEncoder.encode(tokenizedQuery.toString(), StandardCharsets.UTF_8);
    KVSClient kvsMaster = ctx.getKVS();
    if (!kvsMaster.existsRow("cached", queryKey)) {
      FlameRDD queryTok = ctx.parallelize(tokenizedQuery);

      FlamePairRDD cms = queryTok.flatMapToPair(q-> {
        final String qWord = q.substring(0, q.lastIndexOf('='));
        final int idx = Integer.valueOf(q.substring(q.lastIndexOf('=')));
        return (Iterable<FlamePair>) getIndex(qWord, ctx.getKVS()).map(url ->{
          int tf = url.substring(url.lastIndexOf(':')).split(" ").length;
          String urlStr = url.substring(0, url.lastIndexOf(':'));
          String[] pos = new String[Q_LEN];
          for(int i = 0; i < pos.length; i++){
            pos[i] = "0";
          }
          pos[idx] = String.valueOf(tf);
          String kc = Arrays.stream(pos).collect(Collectors.joining(","));
          return new FlamePair(urlStr, kc);
        });
      });
      String[] initVals = new String[Q_LEN];
      cms.foldByKey(Arrays.stream(initVals).collect(Collectors.joining(",")), (acc , url) -> {
        String[] vals = acc.split(",");
        String[] cld = url.split(",");
        assert cld.length == vals.length;
        for(int i = 0; i < cld.length; i++){
          if(!cld[i].equals("0")){
            vals[i] = String.valueOf(Integer.valueOf(cld[i]) +  Integer.valueOf(vals[i]));
          }
        }
        return String.join(",", vals);
      }).flatMapToPair(flamePair -> {
        String url = flamePair._1();
        String line = flamePair._2();
        String[] cvals = line.split(",");
        for(int i = 0; i < Q_LEN; i++){
          double j = Double.valueOf(String.valueOf(ctx.getKVS().get("idfRanks", "ic", cvals[i]) )) * Integer.valueOf(cvals[i]);
          cvals[i] = Double.toString(j);
        }
        return Collections.singletonList(new FlamePair(url, String.join(",", cvals)));
      }).saveAsTable("rankTable");
      kvsMaster.put("cached", queryKey, "table", queryKey);
    } else {
      FlamePairRDD cacheEntries = new FlamePairRDDImpl((FlameContextImpl) ctx, "cached");
      FlamePairRDD newCacheEntries = cacheEntries.flatMapToPair(ent -> {
        if (System.currentTimeMillis() - Long.parseLong(ent._2()) > CACHE_LIFETIME_MINS * 1000) {
          try {
            kvsMaster.delete(ent._1());
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
  }

  private static String getPage(String url, KVSClient kvsClient) {
    try {
      return new String(kvsClient.get("crawl", url, "page"), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "";
    }
  }

  private static Stream<String> getIndex(String keyword, KVSClient kvsClient) {
    /*if (keyword.matches("^\".*\"$")) {
      String kw = keyword.replaceAll("^\"|\"$", "");
      return Arrays.stream(kw.split("\\s")).map(String::translateEscapes)
        .flatMap(w -> getIndex(w, kvsClient)).filter(
          url -> Pattern.compile(Pattern.quote(kw), Pattern.CASE_INSENSITIVE)
            .matcher(getPage(url.replaceAll(":(?:\\d*\\s*)*$", ""), kvsClient)).find());
    }*/
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
