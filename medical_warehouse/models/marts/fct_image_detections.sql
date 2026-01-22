{{
    config(
        materialized='table',
        schema='marts',
        unique_key='detection_id'
    )
}}

WITH detection_data AS (
    SELECT 
        *,
        -- Extract message_id from filename if possible
        CASE 
            WHEN filename ~ '^\d+\.jpg$' THEN CAST(SPLIT_PART(filename, '.', 1) AS INTEGER)
            WHEN filename ~ 'message_\d+' THEN CAST(SPLIT_PART(SPLIT_PART(filename, 'message_', 2), '.', 1) AS INTEGER)
            ELSE NULL
        END as extracted_message_id
    FROM {{ source('external', 'yolo_detections') }}
),

enriched_detections AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY processing_timestamp) as detection_id,
        dd.image_path,
        dd.channel_name,
        dd.filename,
        dd.extracted_message_id,
        dd.detection_count,
        dd.detected_objects,
        dd.medical_objects,
        dd.image_category,
        dd.classification_confidence,
        dd.classification_reason,
        dd.detection_1_class,
        dd.detection_1_confidence,
        dd.detection_2_class,
        dd.detection_2_confidence,
        dd.detection_3_class,
        dd.detection_3_confidence,
        dd.processing_timestamp,
        
        -- Add business logic
        CASE 
            WHEN image_category = 'promotional' THEN 'High Engagement Potential'
            WHEN image_category = 'product_display' THEN 'Product Focus'
            WHEN image_category = 'lifestyle' THEN 'Brand Building'
            ELSE 'Other Content'
        END as content_strategy,
        
        -- Calculate engagement score
        CASE 
            WHEN image_category = 'promotional' THEN classification_confidence * 1.5
            WHEN image_category = 'product_display' THEN classification_confidence * 1.2
            WHEN image_category = 'lifestyle' THEN classification_confidence * 1.0
            ELSE classification_confidence * 0.8
        END as engagement_score,
        
        -- Check if contains medical objects
        CASE 
            WHEN medical_objects LIKE '%medicine_bottle%' 
                 OR medical_objects LIKE '%medical_cup%'
                 OR medical_objects LIKE '%surgical_%' THEN TRUE
            ELSE FALSE
        END as contains_medical_objects

    FROM detection_data dd
    WHERE image_category IS NOT NULL
)

SELECT 
    ed.*,
    -- Join with messages if possible
    fm.message_id,
    fm.channel_key,
    fm.date_key,
    fm.message_text,
    fm.view_count,
    fm.forward_count,
    fm.has_media,
    fm.product_category as message_product_category,
    
    -- Analysis metrics
    CASE 
        WHEN fm.view_count IS NOT NULL AND ed.engagement_score > 0.5 
        THEN fm.view_count * ed.engagement_score
        ELSE NULL
    END as estimated_engagement

FROM enriched_detections ed
LEFT JOIN {{ ref('fct_messages') }} fm 
    ON ed.extracted_message_id = fm.message_id 
    OR (ed.channel_name = (
        SELECT channel_name 
        FROM {{ ref('dim_channels') }} dc 
        WHERE dc.channel_key = fm.channel_key
    ) AND ed.extracted_message_id IS NULL)