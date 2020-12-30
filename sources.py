import os
import json
import random
import string
import shutil
import unittest
from time import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType
from pyspark.sql.functions import udf, col, array, lit


def get_campaign_id(row):
	try:
		row_json = json.loads(row.replace("{{", '{').replace("}}", '}'))
		if 'campaign_id' in row_json:
			return row_json['campaign_id']
		else:
			return None
	except:
		return None


def get_channel_id(row):
	try:
		row_json = json.loads(row.replace("{{", '{').replace("}}", '}'))
		if 'channel_id' in row_json:
			return row_json['channel_id']
		else:
			return None
	except:
		return None


def get_purchase_id(row):
	try:
		row_json = json.loads(row.replace("{{", '{').replace("}}", '}'))
		if 'purchase_id' in row_json:
			return row_json['purchase_id']
		else:
			return None
	except:
		return None


def parse_attributes(df):
	get_purchase_id_udf = udf(lambda x: get_purchase_id(x))
	get_channel_id_udf = udf(lambda x: get_channel_id(x))
	get_campaign_id_udf = udf(lambda x: get_campaign_id(x))

	df = (df.withColumn('campaignId', get_campaign_id_udf(col('attributes')))
		  .withColumn('channelId', get_channel_id_udf(col('attributes')))
		  .withColumn('purchaseId', get_purchase_id_udf(col('attributes')))
		  .drop(col('attributes')))
	df.createOrReplaceTempView('clickstream')
	return df


def get_session_df(spark):
	sessions_df = spark.sql("SELECT userId, eventId, eventType, eventTime, campaignId, channelId "
							"FROM clickstream "
							"WHERE eventType = 'app_open' or eventType = 'app_close'")
	sessions_df.createOrReplaceTempView('sessions')

	sessions_df = spark.sql("""SELECT * FROM
                        (SELECT userId, eventTime as start_session,
                        LEAD (eventTime, 1) OVER (PARTITION BY userId ORDER BY eventTime) as end_session,
                        eventType, campaignId, channelId, eventId as session_id
                        FROM sessions ORDER BY userId, start_session)
                        WHERE eventType = 'app_open';""")
	sessions_df.createOrReplaceTempView('sessions')
	return sessions_df


def provide_sessions_for_purchases(spark):
	df = spark.sql("SELECT * FROM clickstream WHERE eventType = 'purchase'")
	df.createOrReplaceTempView('clickstream_purchases')
	clicks_sessions_df = spark.sql("SELECT clicks.userId, clicks.eventId, sessions.campaignId, sessions.channelId, clicks.purchaseId, sessions.session_id "
                                   "FROM clickstream_purchases clicks JOIN sessions "
                                   "ON (clicks.userId = sessions.userId AND sessions.start_session < clicks.eventTime AND sessions.end_session > clicks.eventTime) "
                                   "ORDER BY userId")
	clicks_sessions_df.createOrReplaceTempView('clicks_sessions')
	return clicks_sessions_df
