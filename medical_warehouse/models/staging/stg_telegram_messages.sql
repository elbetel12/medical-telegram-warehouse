{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH raw_messages AS (
    SELECT 
        message_id,
        channel_name,
        message_date::timestamp as message_date,
        COALESCE(message_text, '') as message_text,
        has_media,
        image_path,
        COALESCE(views, 0) as views,
        COALESCE(forwards, 0) as forwards,
        scraped_at::timestamp as scraped_at
    FROM {{ source('raw', 'telegram_messages') }}
    WHERE message_date IS NOT NULL
),

cleaned_messages AS (
    SELECT 
        message_id,
        -- Clean channel names
        CASE 
            WHEN channel_name ILIKE '%chemed%' THEN 'CheMed'
            WHEN channel_name ILIKE '%lobelia%' THEN 'Lobelia Cosmetics'
            WHEN channel_name ILIKE '%tikvah%' THEN 'Tikvah Pharma'
            ELSE INITCAP(channel_name)
        END as channel_name,
        
        message_date,
        
        -- Clean message text
        TRIM(message_text) as message_text,
        
        -- Calculate message length
        LENGTH(TRIM(message_text)) as message_length,
        
        has_media,
        
        -- Clean image path
        CASE 
            WHEN image_path IS NULL OR image_path = '' THEN NULL
            ELSE image_path
        END as image_path,
        
        -- Ensure non-negative counts
        GREATEST(views, 0) as views,
        GREATEST(forwards, 0) as forwards,
        
        scraped_at
    FROM raw_messages
    -- Remove test messages or empty content
    WHERE NOT (message_text ILIKE '%test%' AND LENGTH(message_text) < 10)
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY message_date) as staging_id,
    *
FROM cleaned_messages