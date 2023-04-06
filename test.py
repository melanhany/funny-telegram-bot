from pymongo import MongoClient
from datetime import datetime, timedelta
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
                '$dateToString': {'format': '%Y-%m-%d', 'date': '$datetime'}
                # "year": {"$year": "$dt"},
                # "month": {"$month": "$dt"},
                # "day": {"$dayOfMonth": "$dt"}
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

    db = client["melanhany"]

    sample_collection = db["sample_collection"]

    # date_range_str = '''
    # {
    #     "dt_from": "2022-09-01T00:00:00",
    #     "dt_upto": "2022-12-31T23:59:00"
    # }
    # '''

    date_range = json_data
    dt_from = datetime.fromisoformat(date_range["dt_from"])
    dt_upto = datetime.fromisoformat(date_range["dt_upto"])

    if (date_range["group_type"]=="hour"):
        result_hour_list = [{"labels": datetime(doc["_id"]["year"], doc["_id"]["month"], doc["_id"]["day"], doc["_id"]["hour"]).isoformat(), "dataset": doc["total_value"]} for doc in agg_by_hour(sample_collection, dt_from, dt_upto)]
            # Extract iso_date and total_value to separate lists
        iso_date_hour = [d["labels"] for d in result_hour_list]
        total_value_hour = [d["dataset"] for d in result_hour_list]
        result_hour_json = json.dumps({"dataset": total_value_hour, "labels": iso_date_hour})
        return result_hour_json
    elif (date_range["group_type"]=="day"):
        # result_day_list = [{"labels": datetime(doc["_id"]["year"], doc["_id"]["month"], doc["_id"]["day"]).isoformat(), "dataset": doc["total_value"]} for doc in agg_by_day(sample_collection, dt_from, dt_upto)]
        # iso_date_day = [d["labels"] for d in result_day_list]
        # total_value_day = [d["dataset"] for d in result_day_list]
        # result_day_json = json.dumps({"dataset": total_value_day, "labels": iso_date_day})
        # return result_day_json
        result = list(agg_by_day(sample_collection, dt_from, dt_upto))
        date_range = dt_upto.date() - dt_from.date()
        date_list = [dt_from.date() + timedelta(days=x) for x in range(date_range.days + 1)]
        date_str_list = [date.strftime('%Y-%m-%d') for date in date_list]
        date_dict = dict.fromkeys(date_str_list, 0)
        result_dict = {item['_id']: item['total_value'] for item in result}
        merged_dict = {**date_dict, **result_dict}
        sorted_dict = dict(sorted(merged_dict.items()))

        # Print the output
        result_day_json = json.dumps({'dataset': list(sorted_dict.values()), 'labels': list(sorted_dict.keys())})
        return result_day_json


    elif (date_range["group_type"]=="month"):
        result_month_list = [{"labels": datetime(doc["_id"]["year"], doc["_id"]["month"], 1).isoformat(), "dataset": doc["total_value"]} for doc in agg_by_month(sample_collection, dt_from, dt_upto)]
        iso_date_month = [d["labels"] for d in result_month_list]
        total_value_month = [d["dataset"] for d in result_month_list]
        result_month_json = json.dumps({"dataset": total_value_month, "labels": iso_date_month})
        return result_month_json

