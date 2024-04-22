
def importAsDf(files):
  file_type = "json"
  # CSV options
  infer_schema = "false"
  first_row_is_header = "false"
  delimiter = ","
  # The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(files)
  
  return df


base_dir="/FileStore/tables/proj1/"

business_df = importAsDf([base_dir + "yelp_academic_dataset_business.json"])
## clean up and filer  business data
business_df = business_df.select(['business_id', 'name', 'categories', 'city', 'state','is_open', 'hours']).dropna()

#display(business_df)

checkin_df = importAsDf([base_dir + "yelp_academic_dataset_checkin.json"])


### checkin join business by business_id => business2;
business2_df = business_df.join(checkin_df, business_df.business_id == checkin_df.business_id, "left").select(business_df.business_id, business_df.name, business_df.categories, business_df.city, business_df.state, business_df.is_open, business_df.hours, checkin_df.date)

#display(business2_df)

### review join tip by busi_id/user_id/date =>  review2
tip_df = importAsDf([base_dir + "yelp_academic_dataset_tip.json"])
tip_df = tip_df.select(['business_id', 'date', 'user_id', 'compliment_count']).dropna()

review_files = ["yelp_academic_dataset_review_xaa.json" , "yelp_academic_dataset_review_xab.json" ,"yelp_academic_dataset_review_xac.json" ,"yelp_academic_dataset_review_xad.json" ]
review_df = importAsDf([base_dir + f for f in review_files])
review_df = review_df.select(['business_id', 'date', 'review_id', 'user_id']).dropna()
review2_df = review_df.join(tip_df, [review_df.business_id==tip_df.business_id , review_df.date==tip_df.date ,review_df.user_id==tip_df.user_id], "left").select(review_df.business_id, review_df.date, review_df.review_id, review_df.user_id, tip_df.compliment_count)

display(review2_df)




user_files = ["yelp_academic_dataset_user_aa.json", "yelp_academic_dataset_user_ab.json"]
user_df = importAsDf([base_dir + f for f in user_files])
user_df = user_df.select("user_id","name","review_count","average_stars").dropna()

### review2 join user by user_id => review3
review3_df = review2_df.join(user_df, review2_df.user_id==user_df.user_id, "left").select(review2_df.business_id, review2_df.date, review2_df.review_id, review2_df.user_id, review2_df.compliment_count, user_df.name, user_df.average_stars).withColumnRenamed("date","review_date").withColumnRenamed("name","user_name")

#display(review3_df)


### business2 join review3 by business_id
flatten_df = business2_df.withColumnRenamed("name","business_name").join(review3_df, business2_df.business_id==review3_df.business_id, "left").drop(review3_df.business_id)

display(flatten_df)

flatten_df.write.format("delta").saveAsTable("flatten_all")

