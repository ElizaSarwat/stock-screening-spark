package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetRanking;
import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;
import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.pairing.AssetMetadataPairing;
import scala.Tuple2;

public class AssessedExercise {

public static void main(String[] args) throws InterruptedException {
		
		//--------------------------------------------------------
	    // Static Configuration
	    //--------------------------------------------------------
		String datasetEndDate = "2020-04-01";
		double volatilityCeiling = 4;
		double peRatioThreshold = 25;
	
		long startTime = System.currentTimeMillis();
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
					.builder()
					.config(conf)
					.getOrCreate();
	
		
		// Get the location of the asset pricing data
		String pricesFile = System.getenv("BIGDATA_PRICES");
		if (pricesFile==null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries
		
		// Get the asset metadata
		String assetsFile = System.getenv("BIGDATA_ASSETS");
		if (assetsFile==null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries
		
		
    	//----------------------------------------
    	// Pre-provided code for loading the data 
    	//----------------------------------------
    	
    	// Create Datasets based on the input files
		
		// Load in the assets, this is a relatively small file
		Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
		//assetRows.printSchema();
		System.err.println(assetRows.first().toString());
		JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());
		
		// Load in the prices, this is a large file (not so much in data size, but in number of records)
    	Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
    	Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
    	Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects
		
	
		AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);
		
		System.out.println(finalRanking.toString());
		
		System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");
		
		Thread.sleep(60000);
		
		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		
		
		
		long endTime = System.currentTimeMillis();
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			
			Instant sinstant = Instant.ofEpochSecond( startTime/1000 );
			Date sdate = Date.from( sinstant );
			
			Instant einstant = Instant.ofEpochSecond( endTime/1000 );
			Date edate = Date.from( einstant );
			
			writer.write("StartTime:"+sdate.toGMTString()+'\n');
			writer.write("EndTime:"+edate.toGMTString()+'\n');
			writer.write("Seconds: "+((endTime-startTime)/1000)+'\n');
			writer.write('\n');
			writer.write(finalRanking.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}


	public static AssetRanking rankInvestments(
            SparkSession spark,
            JavaPairRDD<String, AssetMetadata> assetMetadata,
            Dataset<StockPrice> prices,
            String datasetEndDate,
            double volatilityCeiling,
            double peRatioThreshold) {

        final java.time.Instant endInstant =
                bigdata.util.TimeUtil.fromDate(datasetEndDate);

        // 1) Build (ticker -> (date, close)) up to end date
        org.apache.spark.api.java.JavaPairRDD<String,
                scala.Tuple2<java.time.Instant, Double>> tickerDateClose =
                prices.toJavaRDD()
                        .filter((bigdata.objects.StockPrice p) -> {
                            java.time.Instant priceInstant =
                                    bigdata.util.TimeUtil.fromDate(
                                            p.getYear(),
                                            p.getMonth(),
                                            p.getDay());
                            return !priceInstant.isAfter(endInstant);
                        })
                        .mapToPair(p ->
                                new scala.Tuple2<>(
                                        p.getStockTicker(),
                                        new scala.Tuple2<>(
                                                bigdata.util.TimeUtil.fromDate(
                                                        p.getYear(),
                                                        p.getMonth(),
                                                        p.getDay()),
                                                p.getClosePrice()
                                        )
                                )
                        );

        final int MAX_DAYS = 251;

        // 2) Keep only last 251 closes per ticker
        org.apache.spark.api.java.JavaPairRDD<String,
                java.util.List<scala.Tuple2<java.time.Instant, Double>>> last251ByTicker =
                tickerDateClose.combineByKey(
                        new CreateBoundedList(MAX_DAYS),
                        new MergeValueIntoBoundedList(MAX_DAYS),
                        new MergeBoundedLists(MAX_DAYS)
                );
                    
        // 3) Compute 5-day return + 251-day volatility
        org.apache.spark.api.java.JavaPairRDD<String,
                scala.Tuple2<Double, Double>> indicators =
                last251ByTicker.mapToPair(new ComputeIndicatorsPair());

        // 4) Filter by volatility ceiling
        org.apache.spark.api.java.JavaPairRDD<String,
                scala.Tuple2<Double, Double>> okVol =
                indicators.filter((org.apache.spark.api.java.function.Function<scala.Tuple2<String, scala.Tuple2<Double, Double>>, Boolean>) kv -> {
                    double volatility = kv._2()._2();
                    double assetReturn = kv._2()._1();
                    return Double.isFinite(volatility) && volatility < volatilityCeiling && Double.isFinite(assetReturn);
                });

        // 5) Join with metadata
        org.apache.spark.api.java.JavaPairRDD<String,
                scala.Tuple2<scala.Tuple2<Double, Double>, AssetMetadata>> joined =
                okVol.join(assetMetadata);

        org.apache.spark.api.java.JavaPairRDD<String,
                scala.Tuple2<scala.Tuple2<Double, Double>, AssetMetadata>> candidates =
                joined.filter(new MetadataAndPeFilter(peRatioThreshold));

        // 6) Convert to Candidate and take top 5 by return
        java.util.List<Candidate> top5 =
                candidates.map(new ToCandidateMap())
                        .takeOrdered(5, new CandidateReturnDescComparator());

        // 7) Build ranking
        AssetRanking ranking = new AssetRanking();
        Asset[] rankedAssets = ranking.getAssetRanking();

        for (int i = 0; i < top5.size() && i < rankedAssets.length; i++) {
            Candidate c = top5.get(i);

            AssetFeatures features = new AssetFeatures();
            features.setAssetReturn(c.assetReturn);
            features.setAssetVolitility(c.assetVolatility);
            features.setPeRatio(c.peRatio);

            rankedAssets[i] = new Asset(
                    c.ticker,
                    features,
                    c.name,
                    c.industry,
                    c.sector
            );
        }

        ranking.setAssetRanking(rankedAssets);
        return ranking;
    }
    
    // Helper class to store candidate asset information
    static class Candidate implements Serializable {
        private static final long serialVersionUID = 1L;
        String ticker;
        String name;
        String industry;
        String sector;
        double assetReturn;
        double assetVolatility;
        double peRatio;
        
        Candidate(String ticker, String name, String industry, String sector, 
                double assetReturn, double assetVolatility, double peRatio) {
            this.ticker = ticker;
            this.name = name;
            this.industry = industry;
            this.sector = sector;
            this.assetReturn = assetReturn;
            this.assetVolatility = assetVolatility;
            this.peRatio = peRatio;
        }
    }
}

// Separate class definitions for the functions used in combineByKey and transformations

class CreateBoundedList implements Function<Tuple2<Instant, Double>, List<Tuple2<Instant, Double>>>, Serializable {
    private static final long serialVersionUID = 1L;
    private final int maxDays;
    
    CreateBoundedList(int maxDays) {
        this.maxDays = maxDays;
    }
    
    @Override
    public List<Tuple2<Instant, Double>> call(Tuple2<Instant, Double> v) throws Exception {
        List<Tuple2<Instant, Double>> list = new ArrayList<>();
        list.add(v);
        sortAndTrimToLastN(list, maxDays);
        return list;
    }
    
    private void sortAndTrimToLastN(List<Tuple2<Instant, Double>> list, int n) {
        Collections.sort(list, new Comparator<Tuple2<Instant, Double>>() {
            @Override
            public int compare(Tuple2<Instant, Double> a, Tuple2<Instant, Double> b) {
                return a._1().compareTo(b._1());
            }
        });
        if (list.size() > n) {
            List<Tuple2<Instant, Double>> tail = new ArrayList<>(list.subList(list.size() - n, list.size()));
            list.clear();
            list.addAll(tail);
        }
    }
}

class MergeValueIntoBoundedList implements Function2<List<Tuple2<Instant, Double>>, Tuple2<Instant, Double>, List<Tuple2<Instant, Double>>>, Serializable {
    private static final long serialVersionUID = 1L;
    private final int maxDays;
    
    MergeValueIntoBoundedList(int maxDays) {
        this.maxDays = maxDays;
    }
    
    @Override
    public List<Tuple2<Instant, Double>> call(List<Tuple2<Instant, Double>> acc, Tuple2<Instant, Double> v) throws Exception {
        acc.add(v);
        sortAndTrimToLastN(acc, maxDays);
        return acc;
    }
    
    private void sortAndTrimToLastN(List<Tuple2<Instant, Double>> list, int n) {
        Collections.sort(list, new Comparator<Tuple2<Instant, Double>>() {
            @Override
            public int compare(Tuple2<Instant, Double> a, Tuple2<Instant, Double> b) {
                return a._1().compareTo(b._1());
            }
        });
        if (list.size() > n) {
            List<Tuple2<Instant, Double>> tail = new ArrayList<>(list.subList(list.size() - n, list.size()));
            list.clear();
            list.addAll(tail);
        }
    }
}

class MergeBoundedLists implements Function2<List<Tuple2<Instant, Double>>, List<Tuple2<Instant, Double>>, List<Tuple2<Instant, Double>>>, Serializable {
    private static final long serialVersionUID = 1L;
    private final int maxDays;
    
    MergeBoundedLists(int maxDays) {
        this.maxDays = maxDays;
    }
    
    @Override
    public List<Tuple2<Instant, Double>> call(List<Tuple2<Instant, Double>> a, List<Tuple2<Instant, Double>> b) throws Exception {
        a.addAll(b);
        sortAndTrimToLastN(a, maxDays);
        return a;
    }
    
    private void sortAndTrimToLastN(List<Tuple2<Instant, Double>> list, int n) {
        Collections.sort(list, new Comparator<Tuple2<Instant, Double>>() {
            @Override
            public int compare(Tuple2<Instant, Double> a, Tuple2<Instant, Double> b) {
                return a._1().compareTo(b._1());
            }
        });
        if (list.size() > n) {
            List<Tuple2<Instant, Double>> tail = new ArrayList<>(list.subList(list.size() - n, list.size()));
            list.clear();
            list.addAll(tail);
        }
    }
}

class ComputeIndicatorsPair implements PairFunction<Tuple2<String, List<Tuple2<Instant, Double>>>, String, Tuple2<Double, Double>>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final int TRADING_DAYS = 251;
    
    @Override
    public Tuple2<String, Tuple2<Double, Double>> call(Tuple2<String, List<Tuple2<Instant, Double>>> tickerPrices) throws Exception {
        String ticker = tickerPrices._1();
        List<Tuple2<Instant, Double>> dateClose = tickerPrices._2();
        
        // Mark as invalid if not enough history for volatility calculation
        if (dateClose == null || dateClose.size() < TRADING_DAYS) {
            return new Tuple2<>(ticker, new Tuple2<>(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        }
        
        // Extract close prices in chronological order
        List<Double> closePrices = new ArrayList<>(dateClose.size());
        for (Tuple2<Instant, Double> dc : dateClose) {
            closePrices.add(dc._2());
        }
        
        // Compute 5-day Return and Volatility
        double assetReturn = Returns.calculate(5, closePrices);
        double assetVolatility = Volitility.calculate(closePrices);
        
        return new Tuple2<>(ticker, new Tuple2<>(assetReturn, assetVolatility));
    }
}

class VolatilityFilter implements Function<Tuple2<String, Tuple2<Double, Double>>, Boolean>, Serializable {
    private static final long serialVersionUID = 1L;
    private final double volatilityCeiling;
    
    VolatilityFilter(double volatilityCeiling) {
        this.volatilityCeiling = volatilityCeiling;
    }
    
    @Override
    public Boolean call(Tuple2<String, Tuple2<Double, Double>> tickerFeatures) throws Exception {
        double volatility = tickerFeatures._2()._2();
        double assetReturn = tickerFeatures._2()._1();
        return Double.isFinite(volatility) && volatility < volatilityCeiling && Double.isFinite(assetReturn);
    }
}

class MetadataAndPeFilter implements Function<Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>>, Boolean>, Serializable {
    private static final long serialVersionUID = 1L;
    private final double peRatioThreshold;
    
    MetadataAndPeFilter(double peRatioThreshold) {
        this.peRatioThreshold = peRatioThreshold;
    }
    
    @Override
    public Boolean call(Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> joined) throws Exception {
        AssetMetadata metadata = joined._2()._2();
        double peRatio = metadata.getPriceEarningRatio();
        
        // Filter out assets with missing metadata or P/E ratio issues
        if (metadata.getName() == null || metadata.getIndustry() == null || metadata.getSector() == null) {
            return false;
        }
        
        // Filter out missing P/E (default 0.0) and those >= threshold
        return peRatio > 0.0 && peRatio < peRatioThreshold;
    }
}

class ToCandidateMap implements org.apache.spark.api.java.function.Function<Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>>, AssessedExercise.Candidate>, Serializable {
    private static final long serialVersionUID = 1L;
    
    @Override
    public AssessedExercise.Candidate call(Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> joined) throws Exception {
        String ticker = joined._1();
        Tuple2<Double, Double> features = joined._2()._1();
        AssetMetadata metadata = joined._2()._2();
        
        return new AssessedExercise.Candidate(
            ticker,
            metadata.getName(),
            metadata.getIndustry(),
            metadata.getSector(),
            features._1(), // assetReturn
            features._2(), // assetVolatility
            metadata.getPriceEarningRatio()
        );
    }
}

class CandidateReturnDescComparator implements Comparator<AssessedExercise.Candidate>, Serializable {
    private static final long serialVersionUID = 1L;
    
    @Override
    public int compare(AssessedExercise.Candidate a, AssessedExercise.Candidate b) {
        // Sort by return descending (higher return first)
        return Double.compare(b.assetReturn, a.assetReturn);
    }
}

