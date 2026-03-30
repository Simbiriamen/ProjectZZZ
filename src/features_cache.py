# -*- coding: utf-8 -*-
"""
features_cache.py - Кэширование признаков для ускорения
"""
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

def build_features_cache(engine, client_ids=None):
    """
    Строит кэш признаков для всех клиентов
    Запускается раз в сутки в 05:00
    """
    query = """
    CREATE TABLE IF NOT EXISTS features_cache (
        client_id TEXT,
        sku_id TEXT,
        frequency_30d INTEGER,
        frequency_90d INTEGER,
        days_since_last_purchase INTEGER,
        rolling_sales_2w NUMERIC,
        rolling_sales_4w NUMERIC,
        rolling_sales_8w NUMERIC,
        global_popularity INTEGER,
        portfolio_diversity INTEGER,
        group_trend_6m NUMERIC,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (client_id, sku_id)
    );
    
    TRUNCATE features_cache;
    
    INSERT INTO features_cache (client_id, sku_id, frequency_30d, frequency_90d, 
                                 days_since_last_purchase, rolling_sales_2w, 
                                 rolling_sales_4w, rolling_sales_8w,
                                 global_popularity, portfolio_diversity,
                                 group_trend_6m)
    SELECT DISTINCT ON (se.client_id, se.sku_id)
        se.client_id,
        se.sku_id,
        (SELECT COUNT(*) FROM sales_enriched se2 
         WHERE se2.client_id = se.client_id 
           AND se2.purchase_date >= se.purchase_date - INTERVAL '30 days') AS frequency_30d,
        (SELECT COUNT(*) FROM sales_enriched se2 
         WHERE se2.client_id = se.client_id 
           AND se2.purchase_date >= se.purchase_date - INTERVAL '90 days') AS frequency_90d,
        (SELECT (se.purchase_date - MAX(se2.purchase_date)) 
         FROM sales_enriched se2 
         WHERE se2.client_id = se.client_id 
           AND se2.sku_id = se.sku_id 
           AND se2.purchase_date < se.purchase_date) AS days_since_last_purchase,
        (SELECT AVG(se2.quantity) FROM sales_enriched se2 
         WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
           AND se2.purchase_date >= se.purchase_date - INTERVAL '14 days'
           AND se2.purchase_date < se.purchase_date) AS rolling_sales_2w,
        (SELECT AVG(se2.quantity) FROM sales_enriched se2 
         WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
           AND se2.purchase_date >= se.purchase_date - INTERVAL '28 days'
           AND se2.purchase_date < se.purchase_date) AS rolling_sales_4w,
        (SELECT AVG(se2.quantity) FROM sales_enriched se2 
         WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
           AND se2.purchase_date >= se.purchase_date - INTERVAL '56 days'
           AND se2.purchase_date < se.purchase_date) AS rolling_sales_8w,
        (SELECT COUNT(*) FROM sales_enriched se2 
         WHERE se2.sku_id = se.sku_id
           AND se2.purchase_date >= se.purchase_date - INTERVAL '90 days'
           AND se2.purchase_date <= se.purchase_date) AS global_popularity,
        (SELECT COUNT(DISTINCT se2.category) FROM sales_enriched se2 
         WHERE se2.client_id = se.client_id
           AND se2.purchase_date >= se.purchase_date - INTERVAL '6 months'
           AND se2.purchase_date <= se.purchase_date
           AND se2.category IS NOT NULL) AS portfolio_diversity,
        (SELECT CASE WHEN prev_6m = 0 THEN 0
                ELSE (curr_6m - prev_6m)::NUMERIC / NULLIF(prev_6m, 0) END
         FROM (
             SELECT 
                 COUNT(*) FILTER (WHERE purchase_date >= se.purchase_date - INTERVAL '6 months' 
                                   AND purchase_date < se.purchase_date) AS curr_6m,
                 COUNT(*) FILTER (WHERE purchase_date >= se.purchase_date - INTERVAL '12 months' 
                                   AND purchase_date < se.purchase_date - INTERVAL '6 months') AS prev_6m
             FROM sales_enriched se2
             WHERE se2.client_id = se.client_id AND se2.marketing_group = se.marketing_group
         ) sub) AS group_trend_6m
    FROM sales_enriched se
    WHERE se.client_id = ANY(:client_ids)
    """
    
    with engine.begin() as conn:
        if client_ids:
            conn.execute(text(query), {"client_ids": client_ids})
        else:
            conn.execute(text(query.replace(":client_ids", "ARRAY(SELECT DISTINCT client_id FROM sales_enriched)")))
    
    return True