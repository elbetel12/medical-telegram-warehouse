{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH channel_stats AS (
    SELECT 
        channel_name,
        COUNT(*) as total_posts,
        MIN(message_date) as first_post_date,
        MAX(message_date) as last_post_date,
        AVG(views) as avg_views,
        AVG(forwards) as avg_forwards,
        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as total_media_posts
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
),

channel_types AS (
    SELECT 
        channel_name,
        CASE 
            WHEN channel_name ILIKE '%pharma%' OR channel_name ILIKE '%drug%' THEN 'Pharmaceutical'
            WHEN channel_name ILIKE '%cosmetic%' OR channel_name ILIKE '%beauty%' THEN 'Cosmetics'
            WHEN channel_name ILIKE '%medical%' OR channel_name ILIKE '%health%' THEN 'Medical'
            ELSE 'Other'
        END as channel_type
    FROM channel_stats
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['cs.channel_name']) }} as channel_key,
    cs.channel_name,
    ct.channel_type,
    cs.first_post_date,
    cs.last_post_date,
    cs.total_posts,
    ROUND(cs.avg_views::numeric, 2) as avg_views,
    ROUND(cs.avg_forwards::numeric, 2) as avg_forwards,
    cs.total_media_posts,
    ROUND((cs.total_media_posts::float / NULLIF(cs.total_posts, 0)) * 100, 2) as media_percentage
FROM channel_stats cs
LEFT JOIN channel_types ct ON cs.channel_name = ct.channel_name