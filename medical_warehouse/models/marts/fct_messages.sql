{{
    config(
        materialized='table',
        schema='marts',
        unique_key='message_id'
    )
}}

SELECT 
    stm.message_id,
    dc.channel_key,
    dd.date_key,
    stm.message_text,
    stm.message_length,
    stm.views as view_count,
    stm.forwards as forward_count,
    stm.has_media,
    stm.image_path,
    stm.scraped_at,
    
    -- Extract potential product mentions (simple keyword matching)
    CASE 
        WHEN stm.message_text ILIKE '%paracetamol%' OR stm.message_text ILIKE '%acetaminophen%' THEN 'Paracetamol'
        WHEN stm.message_text ILIKE '%antibiotic%' OR stm.message_text ILIKE '%amoxicillin%' THEN 'Antibiotics'
        WHEN stm.message_text ILIKE '%vitamin%' OR stm.message_text ILIKE '%supplement%' THEN 'Vitamins'
        WHEN stm.message_text ILIKE '%cream%' OR stm.message_text ILIKE '%ointment%' THEN 'Topicals'
        WHEN stm.message_text ILIKE '%syrup%' OR stm.message_text ILIKE '%liquid%' THEN 'Liquids'
        ELSE 'Other'
    END as product_category,
    
    -- Price indicator (if mentioned in text)
    CASE 
        WHEN stm.message_text ~ '\$\d+(\.\d{2})?' 
             OR stm.message_text ~ '\d+\s*(birr|ETB|USD)' 
             OR stm.message_text ~ '\d+\s*(price|cost)' THEN TRUE
        ELSE FALSE
    END as has_price_mention,
    
    -- Urgency indicator
    CASE 
        WHEN stm.message_text ILIKE '%urgent%' 
             OR stm.message_text ILIKE '%emergency%'
             OR stm.message_text ILIKE '%limited stock%'
             OR stm.message_text ILIKE '%last chance%' THEN TRUE
        ELSE FALSE
    END as has_urgency_indicator
    
FROM {{ ref('stg_telegram_messages') }} stm
LEFT JOIN {{ ref('dim_channels') }} dc ON stm.channel_name = dc.channel_name
LEFT JOIN {{ ref('dim_dates') }} dd ON DATE(stm.message_date) = dd.full_date