-- Test to ensure message dates are within reasonable range
SELECT 
    message_id,
    channel_name,
    message_date
FROM {{ ref('stg_telegram_messages') }}
WHERE 
    message_date < '2020-01-01'  -- Before Telegram's widespread adoption in Ethiopia
    OR message_date > CURRENT_TIMESTAMP + INTERVAL '1 day'  -- Future dates