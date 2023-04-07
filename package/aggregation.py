from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import os
import json

def agg_by_hour(sample_collection, dt_from, dt_upto):
    # Group and aggregate data by hour
    pipeline_hour = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {
                "year": {"$year": "$dt"},
                "month": {"$month": "$dt"},
                "day": {"$dayOfMonth": "$dt"},
                "hour": {"$hour": "$dt"}
            },
            "total_value": {"$sum": "$value"}
        }},
        {"$sort": {"_id": 1}}
    ]
    result_hour = sample_collection.aggregate(pipeline_hour)
    return result_hour

def agg_by_day(sample_collection, dt_from, dt_upto):
    # Group and aggregate data by day
    pipeline_day = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {
                "year": {"$year": "$dt"},
                "month": {"$month": "$dt"},
                "day": {"$dayOfMonth": "$dt"}
            },
            "total_value": {"$sum": "$value"}
        }},
        {"$sort": {"_id": 1}}
    ]
    result_day = sample_collection.aggregate(pipeline_day)
    return result_day

def agg_by_month(sample_collection, dt_from, dt_upto):
    # Group and aggregate data by month
    pipeline_month = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {
                "year": {"$year": "$dt"},
                "month": {"$month": "$dt"}
            },
            "total_value": {"$sum": "$value"}
        }},
        {"$sort": {"_id": 1}}
    ]
    result_month = sample_collection.aggregate(pipeline_month)
    return result_month

def aggregate_data(json_data):
    client = MongoClient()

    db = client[os.getenv('DB_NAME')]

    sample_collection = db[os.getenv('COLLECTION_NAME')]

    date_range = json_data
    dt_from = datetime.fromisoformat(date_range["dt_from"])
    dt_upto = datetime.fromisoformat(date_range["dt_upto"])

    if (date_range["group_type"]=="hour"):
        if (dt_upto.minute > 0):
            result_hour_list = [{"labels": datetime(doc["_id"]["year"], doc["_id"]["month"], doc["_id"]["day"], doc["_id"]["hour"]).isoformat(), "dataset": doc["total_value"]} for doc in agg_by_hour(sample_collection, dt_from, dt_upto)]
            iso_date_hour = [d["labels"] for d in result_hour_list]
            total_value_hour = [d["dataset"] for d in result_hour_list]
            result_hour_json = json.dumps({"dataset": total_value_hour, "labels": iso_date_hour})
            return result_hour_json
        else:

            result_hour = agg_by_hour(sample_collection, dt_from, dt_upto)
            result_dict = {}

            for r in result_hour:
                result_dict[(r['_id']['year'], r['_id']['month'], r['_id']['day'], r['_id']['hour'])] = r['total_value']

            # Add missing datetime records with 0 value
            curr_hour = dt_from.replace(minute=0, second=0, microsecond=0)
            end_hour = dt_upto + timedelta(hours=1)
            iso_date_hour = []
            total_value_hour = []
            while curr_hour < end_hour:
                iso_date_hour.append(curr_hour.isoformat())
                hour_key = (curr_hour.year, curr_hour.month, curr_hour.day, curr_hour.hour)
                if hour_key in result_dict:
                    total_value_hour.append(result_dict[hour_key])
                else:
                    total_value_hour.append(0)
                curr_hour += timedelta(hours=1)

            result_hour_json = json.dumps({"dataset": total_value_hour, "labels": iso_date_hour})
            return result_hour_json

    elif (date_range["group_type"]=="day"):
        # result_day_list = [{"labels": datetime(doc["_id"]["year"], doc["_id"]["month"], doc["_id"]["day"]).isoformat(), "dataset": doc["total_value"]} for doc in agg_by_day(sample_collection, dt_from, dt_upto)]
        # iso_date_day = [d["labels"] for d in result_day_list]
        # total_value_day = [d["dataset"] for d in result_day_list]
        # result_day_json = json.dumps({"dataset": total_value_day, "labels": iso_date_day})
        # return result_day_json
        
        result_day = agg_by_day(sample_collection, dt_from, dt_upto)
        
        result_dict = {}
        for r in result_day:
            result_dict[(r['_id']['year'], r['_id']['month'], r['_id']['day'])] = r['total_value']

        # Fill missing dates with 0 value
        date_range = pd.date_range(dt_from.date(), dt_upto.date(), freq='D')
        result_list = [{"labels": d.isoformat(), "dataset": result_dict.get((d.year, d.month, d.day), 0)} for d in date_range]

        iso_date_day = [d["labels"] for d in result_list]
        total_value_day = [d["dataset"] for d in result_list]
        result_day_json = json.dumps({"dataset": total_value_day, "labels": iso_date_day})
        return result_day_json


    elif (date_range["group_type"]=="month"):
        result_month_list = [{"labels": datetime(doc["_id"]["year"], doc["_id"]["month"], 1).isoformat(), "dataset": doc["total_value"]} for doc in agg_by_month(sample_collection, dt_from, dt_upto)]
        iso_date_month = [d["labels"] for d in result_month_list]
        total_value_month = [d["dataset"] for d in result_month_list]
        result_month_json = json.dumps({"dataset": total_value_month, "labels": iso_date_month})
        return result_month_json
       

