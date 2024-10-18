%scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

// SparkSession 생성
val spark = SparkSession
  .builder()
  .appName("S3 CSV Read Example")
  .config("spark.master", "local")
  .getOrCreate()


import spark.implicits._

// S3 경로 정의
val inputS3Path = "s3://test/kaggle/2019-Oct.csv"
val outputS3Path = "s3://test/processed/kaggle/parquet/"
val externalTableName = "kaggle_external_table"

// External Table 존재 여부 확인
val tableExists = spark.catalog.tableExists(externalTableName)

if (!tableExists) {
  println("기존 테이블이 존재하지 않습니다. 전체 데이터를 처리합니다.")

  // 전체 CSV 파일 읽기
  val df = spark.read
    .option("header", "true") 
    .option("inferSchema", "true")
    .csv(inputS3Path)
    .withColumn("timestamp_kst", from_utc_timestamp(col("event_time"), "Asia/Seoul"))
    .withColumn("date", to_date(col("timestamp_kst")))

  // 데이터 저장
  df.write
    .mode(SaveMode.Overwrite)
    .partitionBy("date")
    .option("compression", "snappy")
    .parquet(outputS3Path)

  // External Table 생성
  spark.sql(s"""
    CREATE EXTERNAL TABLE IF NOT EXISTS $externalTableName
    USING PARQUET
    OPTIONS (
      path "$outputS3Path"
    )
  """)
} else {
  println("기존 테이블이 존재합니다. 날짜별 행 수를 비교합니다.")

  // 전체 CSV 파일 읽기
  println("CSV 파일을 읽어옵니다.")
  val csvDF = spark.read
    .option("header", "true") 
    .option("inferSchema", "true")
    .csv(inputS3Path)
    .withColumn("timestamp_kst", from_utc_timestamp(col("event_time"), "Asia/Seoul"))
    .withColumn("date", to_date(col("timestamp_kst")))

  // CSV 데이터의 날짜별 행 수 계산
  println("CSV 데이터의 날짜별 행 수를 계산합니다.")
  val csvCounts = csvDF.groupBy("date").count().withColumnRenamed("count", "csv_count")


  // 기존 테이블에서 날짜별 행 수 계산
  println("기존 테이블의 날짜별 행 수를 계산합니다.")
  val tableDF = spark.read.parquet(outputS3Path)
  val tableCounts = tableDF.groupBy("date").count().withColumnRenamed("count", "table_count")

  // 날짜별로 CSV와 테이블의 행 수를 비교
  println("CSV와 테이블의 날짜별 행 수를 비교합니다.")
  val countComparison = csvCounts.join(tableCounts, Seq("date"), "outer")
    .na.fill(0) 
    .withColumn("needs_update", col("csv_count") =!= col("table_count"))
  countComparison.show()

  // 업데이트가 필요한 날짜 추출
  println("업데이트가 필요한 날짜를 추출합니다.")
  val datesToUpdate = countComparison.filter(col("needs_update") === true)
    .select("date")
    .collect()
    .map(row => row.getAs[java.sql.Date]("date"))
    
  println(s"업데이트가 필요한 날짜: ${datesToUpdate.mkString(", ")}")

  if (datesToUpdate.nonEmpty) {
    // 업데이트가 필요한 날짜의 데이터만 필터링
    println("업데이트가 필요한 날짜의 데이터를 필터링합니다.")
    val dfToUpdate = csvDF.filter(col("date").isin(datesToUpdate: _*))

    // 필터링된 데이터 확인
    println("필터링된 데이터의 날짜별 행 수:")
    dfToUpdate.groupBy("date").count().show()

    // 동적 파티션 덮어쓰기 설정
    println("동적 파티션 덮어쓰기 모드를 설정합니다.")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    // 해당 날짜의 파티션 덮어쓰기
    println("해당 날짜의 파티션을 덮어씁니다.")
    dfToUpdate.write
      .mode(SaveMode.Overwrite)
      .partitionBy("date")
      .option("compression", "snappy")
      .parquet(outputS3Path)

    // 테이블 리프레시
    println("테이블을 리프레시합니다.")
    spark.catalog.refreshTable(externalTableName)
  } else {
    println("모든 날짜의 데이터가 최신 상태입니다. 추가 작업이 필요하지 않습니다.")
  }
}
