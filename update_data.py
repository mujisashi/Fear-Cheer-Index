import requests
import json
from datetime import datetime, timedelta, timezone
import os
import statistics

from hubspot_api_request import hubspot_api_request

# API Configuration
API_URL = "https://starscape.infegy.com/api/query/agg/"

def get_api_token():
    """Read API token from file"""
    try:
        with open('api_token.txt', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("Error: api_token.txt file not found")
        exit(1)
    except Exception as e:
        print(f"Error reading API token: {str(e)}")
        exit(1)

API_TOKEN = get_api_token()


def _load_disclaimer_blocklist():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries", "keywords", "disclaimer_blocklist.txt")
    with open(path) as f:
        return [line.rstrip("\n") for line in f if line.strip()]


DISCLAIMER_BLOCKLIST = _load_disclaimer_blocklist()

# Query IDs (swap these to repoint metrics at new Starscape queries)
TOTAL_VOLUME_QUERY_ID = "q_WoKuQA6Dr45"
HOPE_QUERY_ID = "q_eT1MK2IoiMc"
DESPAIR_QUERY_ID = "q_ghefZgLHgh4"
ESI_QUERY_ID = "q_umyTj9rRaE4"
FINANCIAL_ANXIETY_QUERY_ID = "q_BmuWhdVqEd4"
LAYOFF_MENTIONS_QUERY_ID = "q_dQmy5bWuwp4"
CONSUMER_BEHAVIOR_QUERY_ID = "q_3G5HeNu0XK6"

BASE_QUERY = {
    "op": "and",
    "values": [
        {
            "op": "contains",
            "field": "country",
            "value": "us"
        },
        {
            "op": ">",
            "field": "published",
            "value": "-P2M"
        },
        {
            "op": "not",
            "values": [
                {
                    "op": "contains",
                    "field": "channels",
                    "values": ["lexisnexis"]
                }
            ]
        },
        {
            "op": "not",
            "values": [
                {
                    "op": "contains",
                    "fields": ["title", "body"],
                    "values": DISCLAIMER_BLOCKLIST
                }
            ]
        }
    ]
}

# Query IDs and their corresponding metrics and aggregations
QUERY_METRICS = {
    TOTAL_VOLUME_QUERY_ID: {
        "metric_name": "Total Volume",
        "aggs": {
            "daily_volume": {
                "op": "histogram",
                "field": "published",
                "interval": "day",
            }
        }
    },
    HOPE_QUERY_ID: {
        "metric_name": "hope",
        "aggs": {
            "daily_volume": {
                "op": "histogram",
                "field": "published",
                "interval": "day",
            },
            "sentiment": {
                "op": "keyword",
                "field": "sentiment"
            }
        }
    },
    DESPAIR_QUERY_ID: {
        "metric_name": "despair",
        "aggs": {
            "daily_volume": {
                "op": "histogram",
                "field": "published",
                "interval": "day",
            },
            "sentiment": {
                "op": "keyword",
                "field": "sentiment"
            }
        }
    },
    ESI_QUERY_ID: {
        "metric_name": "esi",
        "aggs": {
            "daily_volume": {
                "op": "histogram",
                "field": "published",
                "interval": "day",
            },
            "sentiment": {
                "op": "keyword",
                "field": "sentiment"
            }
        }
    },
    FINANCIAL_ANXIETY_QUERY_ID: {
        "metric_name": "financialAnxiety",
        "aggs": {
            "daily_volume": {
                "op": "histogram",
                "field": "published",
                "interval": "day",
            },
            "sentiment": {
                "op": "keyword",
                "field": "sentiment"
            }
        }
    },
    LAYOFF_MENTIONS_QUERY_ID: {
        "metric_name": "layoffMentions",
        "aggs": {
            "daily_volume": {
                "op": "histogram",
                "field": "published",
                "interval": "day",
            },
            "sentiment": {
                "op": "keyword",
                "field": "sentiment"
            }
        }
    },
    CONSUMER_BEHAVIOR_QUERY_ID: {
        "metric_name": "consumerBehavior",
        "aggs": {
            "daily_volume": {
                "op": "histogram",
                "field": "published",
                "interval": "day",
            },
            "sentiment": {
                "op": "keyword",
                "field": "sentiment"
            }
        }
    }
}

def fetch_infegy_data(query_id):
    """Fetch sentiment data from Infegy API for a specific query"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_TOKEN}"
    }
    
    payload = {
        "dataset_id": "ds_gj4u3F40SLa",
        "timezone": "Etc/UTC",
        "aggs": QUERY_METRICS[query_id]["aggs"],
        "query": BASE_QUERY
    }
    
    url = API_URL if query_id == TOTAL_VOLUME_QUERY_ID else f"{API_URL}/{query_id}"
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def calculate_percentage(mention_count, total_volume):
    """Calculate percentage of total volume"""
    return (mention_count / total_volume * 100) if total_volume > 0 else 0

def process_query(query_id, total_volume_data=None):
    """Process a single query and return metric values and labels"""
    try:
        data = fetch_infegy_data(query_id)
        metric_values = []
        labels = []
        raw_counts = []
        net_sentiment = []
        
        metric_name = QUERY_METRICS[query_id]["metric_name"]
        
        is_ratio_metric = metric_name in ["hope", "despair"]
        is_percentage_metric = metric_name in ["layoffMentions", "consumerBehavior", "hope", "despair", "esi", "financialAnxiety"]
        
        # Calculate sentiment once for the entire period
        if 'sentiment' in data and '_buckets' in data['sentiment']:
            sentiment_buckets = data['sentiment']['_buckets']
            positive_count = sum(bucket['_count'] for bucket in sentiment_buckets 
                               if bucket['_key'] in ['p', 'positive'])
            negative_count = sum(bucket['_count'] for bucket in sentiment_buckets 
                               if bucket['_key'] in ['n', 'negative'])
            total_sentiment = positive_count + negative_count
            net_sentiment_value = (positive_count / total_sentiment) if total_sentiment > 0 else 0
        else:
            net_sentiment_value = 0
        
        for day in data['daily_volume']['_buckets']:
            date = datetime.fromisoformat(day['_key'].replace('Z', '+00:00'))
            labels.append(date.strftime("%b %d"))
            
            mention_count = day['_count']
            
            if is_ratio_metric:
                raw_counts.append(mention_count)
            
            if is_percentage_metric and total_volume_data:
                total_volume = next(
                    (bucket['_count'] for bucket in total_volume_data['daily_volume']['_buckets'] 
                     if bucket['_key'] == day['_key']),
                    0
                )
                value = calculate_percentage(mention_count, total_volume)
                metric_values.append(round(value, 3))
            else:
                metric_values.append(mention_count)
            
            net_sentiment.append(round(net_sentiment_value, 3))
        
        return metric_values, labels, raw_counts if is_ratio_metric else None, net_sentiment
    except Exception as e:
        print(f"Error processing query {query_id}: {str(e)}")
        return None, None, None, None

def calculate_growth_metrics(values):
    """Calculate growth metrics based on trend data"""
    if not values or len(values) < 2:
        return {
            "short_term": 0,
            "long_term": 0,
            "direction": "neutral"
        }
    
    # Calculate short-term growth (last 7 days)
    short_term_values = values[-7:] if len(values) >= 7 else values
    if len(short_term_values) >= 2:
        short_term_growth = ((short_term_values[-1] - short_term_values[0]) / short_term_values[0] * 100) if short_term_values[0] != 0 else 0
    else:
        short_term_growth = 0
    
    # Calculate long-term growth (last 30 days)
    long_term_values = values[-30:] if len(values) >= 30 else values
    if len(long_term_values) >= 2:
        long_term_growth = ((long_term_values[-1] - long_term_values[0]) / long_term_values[0] * 100) if long_term_values[0] != 0 else 0
    else:
        long_term_growth = 0
    
    # Determine trend direction based on short-term growth
    direction = "up" if short_term_growth > 0 else "down" if short_term_growth < 0 else "neutral"
    
    return {
        "short_term": round(short_term_growth, 1),
        "long_term": round(long_term_growth, 1),
        "direction": direction
    }

def update_data_file(metric_data):
    """Update data.json with new metric data"""
    try:
        try:
            with open('data.json', 'r') as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {"lastUpdated": "", "metrics": {}}
        
        if 'metrics' not in data:
            data['metrics'] = {}
        
        # Remove any existing hopeDispair metric
        if 'hopeDispair' in data['metrics']:
            del data['metrics']['hopeDispair']
        
        # Update metrics
        for metric_name, (values, labels, _, net_sentiment) in metric_data.items():
            if metric_name not in data['metrics']:
                data['metrics'][metric_name] = {}
            
            # Calculate average net sentiment across all days
            avg_net_sentiment = sum(net_sentiment) / len(net_sentiment) if net_sentiment else 0
            
            # Calculate growth metrics
            growth = calculate_growth_metrics(values)
            
            data['metrics'][metric_name].update({
                'current': values[-1],
                'trend': values,
                'labels': labels,
                'net_sentiment': round(avg_net_sentiment * 100, 1),  # Convert to percentage and round to 1 decimal place
                'growth': growth
            })
        
        data['lastUpdated'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        hubspot_api_request(data)
        
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        
    except Exception as e:
        print(f"Error updating data.json: {str(e)}")

def calculate_ratio_metric(hope_counts, despair_counts, labels):
    """Calculate the hope/despair ratio metric"""
    ratio_values = []
    for h, d in zip(hope_counts, despair_counts):
        ratio = h / d if d > 0 else 0
        ratio_values.append(round(ratio, 3))
    return ratio_values, labels

def main():
    try:
        print("Starting data update process...")
        metric_data = {}
        raw_counts = {}
        
        print("Fetching total volume data...")
        total_volume_data = fetch_infegy_data(TOTAL_VOLUME_QUERY_ID)
        
        for query_id, query_config in QUERY_METRICS.items():
            if query_id == TOTAL_VOLUME_QUERY_ID:
                continue
                
            values, labels, counts, net_sentiment = process_query(query_id, total_volume_data)
            if values and labels:
                metric_name = query_config["metric_name"]
                metric_data[metric_name] = (values, labels, counts, net_sentiment)
                if counts is not None:
                    raw_counts[metric_name] = counts
        
        if "hope" in raw_counts and "despair" in raw_counts:
            print("Calculating hope/despair ratio...")
            ratio_values, labels = calculate_ratio_metric(
                raw_counts["hope"],
                raw_counts["despair"],
                metric_data["hope"][1]  # Use hope's labels
            )
            # For hopeDespairRatio, we'll use the average of hope and despair net sentiment
            hope_net_sentiment = metric_data["hope"][3]
            despair_net_sentiment = metric_data["despair"][3]
            ratio_net_sentiment = [(h + d) / 2 for h, d in zip(hope_net_sentiment, despair_net_sentiment)]
            metric_data["hopeDespairRatio"] = (ratio_values, labels, None, ratio_net_sentiment)
        
        if metric_data:
            update_data_file(metric_data)
            print("Data update completed successfully!")
        else:
            print("No data was updated.")
            
    except Exception as e:
        print(f"Error updating data: {str(e)}")

if __name__ == "__main__":
    main() 