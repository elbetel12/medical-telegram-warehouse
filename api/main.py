"""
FastAPI Analytical API for Ethiopian Medical Telegram Data
Exposes insights from the data warehouse through RESTful endpoints
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import pandas as pd
from loguru import logger

from .database import get_db, Database
from .schemas import (
    TopProductsResponse, ChannelActivityResponse, MessageSearchResponse,
    VisualContentResponse, ProductTrendResponse, ChannelComparisonResponse
)

# Initialize FastAPI app
app = FastAPI(
    title="Ethiopian Medical Telegram Analytics API",
    description="API for analyzing Ethiopian medical business data from Telegram channels",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database
db = Database()

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup"""
    try:
        db.connect()
        logger.success("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection on shutdown"""
    db.disconnect()
    logger.info("Database connection closed")

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint - API information"""
    return {
        "message": "Ethiopian Medical Telegram Analytics API",
        "version": "1.0.0",
        "documentation": "/docs",
        "endpoints": {
            "analytics": "/api/reports/",
            "channels": "/api/channels/",
            "search": "/api/search/",
            "health": "/health"
        }
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        db.cursor.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "database": db_status,
        "version": "1.0.0"
    }

# Analytics Endpoints
@app.get("/api/reports/top-products", 
         response_model=List[TopProductsResponse],
         tags=["Analytics"],
         summary="Get top mentioned medical products")
async def get_top_products(
    limit: int = Query(10, description="Number of top products to return", ge=1, le=100),
    days: int = Query(30, description="Number of days to analyze", ge=1, le=365),
    channel_name: Optional[str] = Query(None, description="Filter by specific channel")
):
    """
    Returns the most frequently mentioned medical products across Telegram channels.
    
    - **limit**: Number of top products to return
    - **days**: Analysis period in days
    - **channel_name**: Filter by specific channel
    """
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Build query
        query = """
            WITH product_mentions AS (
                SELECT 
                    product_category,
                    COUNT(*) as mention_count,
                    COUNT(DISTINCT channel_key) as channel_count,
                    AVG(view_count) as avg_views,
                    SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as media_count
                FROM marts.fct_messages
                WHERE date_key IN (
                    SELECT date_key 
                    FROM marts.dim_dates 
                    WHERE full_date BETWEEN %s AND %s
                )
        """
        
        params = [start_date.date(), end_date.date()]
        
        if channel_name:
            query += """
                AND channel_key IN (
                    SELECT channel_key 
                    FROM marts.dim_channels 
                    WHERE channel_name = %s
                )
            """
            params.append(channel_name)
        
        query += """
                GROUP BY product_category
                HAVING product_category != 'Other'
            )
            SELECT 
                product_category,
                mention_count,
                channel_count,
                ROUND(avg_views::numeric, 2) as avg_views,
                media_count,
                ROUND((media_count::float / NULLIF(mention_count, 0)) * 100, 2) as media_percentage
            FROM product_mentions
            ORDER BY mention_count DESC
            LIMIT %s;
        """
        params.append(limit)
        
        # Execute query
        db.cursor.execute(query, params)
        results = db.cursor.fetchall()
        
        # Format response
        response = []
        for row in results:
            response.append({
                "product_category": row[0],
                "mention_count": row[1],
                "channel_count": row[2],
                "avg_views": float(row[3]) if row[3] else 0.0,
                "media_count": row[4],
                "media_percentage": float(row[5]) if row[5] else 0.0,
                "trend": "increasing" if row[1] > 100 else "stable"  # Simplified trend
            })
        
        logger.info(f"Top products query returned {len(response)} results")
        return response
        
    except Exception as e:
        logger.error(f"Error in top-products endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/channels/{channel_name}/activity",
         response_model=ChannelActivityResponse,
         tags=["Channels"],
         summary="Get channel posting activity and trends")
async def get_channel_activity(
    channel_name: str,
    period: str = Query("weekly", description="Analysis period", regex="^(daily|weekly|monthly)$"),
    days: int = Query(30, description="Number of days to analyze", ge=1, le=365)
):
    """
    Returns posting activity and trends for a specific Telegram channel.
    
    - **channel_name**: Name of the Telegram channel
    - **period**: Aggregation period (daily, weekly, monthly)
    - **days**: Analysis period in days
    """
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Determine group by clause based on period
        if period == "daily":
            group_by = "dd.full_date"
            date_format = "YYYY-MM-DD"
        elif period == "weekly":
            group_by = "DATE_TRUNC('week', dd.full_date)"
            date_format = "IYYY-\"W\"IW"
        else:  # monthly
            group_by = "DATE_TRUNC('month', dd.full_date)"
            date_format = "YYYY-MM"
        
        query = f"""
            SELECT 
                TO_CHAR({group_by}, '{date_format}') as period,
                COUNT(fm.message_id) as post_count,
                SUM(fm.view_count) as total_views,
                AVG(fm.view_count) as avg_views,
                SUM(fm.forward_count) as total_forwards,
                SUM(CASE WHEN fm.has_media THEN 1 ELSE 0 END) as media_posts,
                COUNT(DISTINCT fm.product_category) as unique_products
            FROM marts.fct_messages fm
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            WHERE dc.channel_name = %s
                AND dd.full_date BETWEEN %s AND %s
            GROUP BY {group_by}
            ORDER BY {group_by};
        """
        
        # Execute query
        db.cursor.execute(query, (channel_name, start_date.date(), end_date.date()))
        results = db.cursor.fetchall()
        
        if not results:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found or no data available")
        
        # Calculate trends
        periods = [row[0] for row in results]
        post_counts = [row[1] for row in results]
        
        # Simple trend calculation
        if len(post_counts) > 1:
            recent_avg = sum(post_counts[-3:]) / min(3, len(post_counts))
            previous_avg = sum(post_counts[-6:-3]) / min(3, len(post_counts) - 3) if len(post_counts) > 3 else post_counts[0]
            trend_direction = "increasing" if recent_avg > previous_avg else "decreasing"
            trend_percentage = abs((recent_avg - previous_avg) / previous_avg * 100) if previous_avg > 0 else 0
        else:
            trend_direction = "stable"
            trend_percentage = 0
        
        # Format response
        activity_data = []
        for row in results:
            activity_data.append({
                "period": row[0],
                "post_count": row[1],
                "total_views": row[2],
                "avg_views": float(row[3]) if row[3] else 0.0,
                "total_forwards": row[4],
                "media_posts": row[5],
                "unique_products": row[6]
            })
        
        # Get channel info
        db.cursor.execute("""
            SELECT 
                channel_name, channel_type, total_posts, avg_views, media_percentage
            FROM marts.dim_channels
            WHERE channel_name = %s;
        """, (channel_name,))
        
        channel_info = db.cursor.fetchone()
        
        response = {
            "channel_name": channel_name,
            "channel_type": channel_info[1] if channel_info else "Unknown",
            "analysis_period": f"last_{days}_days",
            "period_granularity": period,
            "total_posts": sum(item["post_count"] for item in activity_data),
            "avg_posts_per_period": sum(item["post_count"] for item in activity_data) / len(activity_data) if activity_data else 0,
            "trend_direction": trend_direction,
            "trend_percentage": round(trend_percentage, 2),
            "activity_data": activity_data
        }
        
        logger.info(f"Channel activity query for '{channel_name}' returned {len(activity_data)} periods")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in channel-activity endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/messages",
         response_model=List[MessageSearchResponse],
         tags=["Search"],
         summary="Search for messages containing specific keywords")
async def search_messages(
    query: str = Query(..., description="Search keyword"),
    channel_name: Optional[str] = Query(None, description="Filter by channel"),
    limit: int = Query(20, description="Maximum number of results", ge=1, le=100),
    offset: int = Query(0, description="Result offset for pagination", ge=0)
):
    """
    Searches for Telegram messages containing specific keywords.
    
    - **query**: Search keyword
    - **channel_name**: Filter by channel
    - **limit**: Maximum results
    - **offset**: Pagination offset
    """
    try:
        # Build search query
        search_query = """
            SELECT 
                fm.message_id,
                dc.channel_name,
                dd.full_date as message_date,
                fm.message_text,
                fm.view_count,
                fm.forward_count,
                fm.has_media,
                fm.product_category,
                ROUND(fm.view_count::numeric / NULLIF(SUM(fm.view_count) OVER (PARTITION BY fm.product_category), 0) * 100, 2) as popularity_percentage
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE fm.message_text ILIKE %s
        """
        
        params = [f"%{query}%"]
        
        if channel_name:
            search_query += " AND dc.channel_name = %s"
            params.append(channel_name)
        
        search_query += """
            ORDER BY fm.view_count DESC
            LIMIT %s OFFSET %s;
        """
        params.extend([limit, offset])
        
        # Execute query
        db.cursor.execute(search_query, params)
        results = db.cursor.fetchall()
        
        # Get total count for pagination
        count_query = """
            SELECT COUNT(*)
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            WHERE fm.message_text ILIKE %s
        """
        count_params = [f"%{query}%"]
        
        if channel_name:
            count_query += " AND dc.channel_name = %s"
            count_params.append(channel_name)
        
        db.cursor.execute(count_query, count_params)
        total_count = db.cursor.fetchone()[0]
        
        # Format response
        response = []
        for row in results:
            # Truncate long message text
            message_text = row[3]
            if len(message_text) > 200:
                message_text = message_text[:197] + "..."
            
            response.append({
                "message_id": row[0],
                "channel_name": row[1],
                "message_date": row[2].isoformat() if row[2] else None,
                "message_preview": message_text,
                "view_count": row[4],
                "forward_count": row[5],
                "has_media": row[6],
                "product_category": row[7],
                "popularity_percentage": float(row[8]) if row[8] else 0.0,
                "relevance_score": min(1.0, len(query) / len(message_text) * 10)  # Simplified relevance
            })
        
        logger.info(f"Message search for '{query}' returned {len(response)} results")
        
        # Add pagination headers
        headers = {
            "X-Total-Count": str(total_count),
            "X-Page-Size": str(limit),
            "X-Page-Offset": str(offset)
        }
        
        return JSONResponse(content=response, headers=headers)
        
    except Exception as e:
        logger.error(f"Error in message-search endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reports/visual-content",
         response_model=VisualContentResponse,
         tags=["Analytics"],
         summary="Get statistics about image usage across channels")
async def get_visual_content_stats(
    days: int = Query(30, description="Number of days to analyze", ge=1, le=365)
):
    """
    Returns statistics about image usage and effectiveness across Telegram channels.
    
    - **days**: Analysis period in days
    """
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Query 1: Overall visual content statistics
        overall_query = """
            SELECT 
                COUNT(*) as total_messages,
                SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as messages_with_media,
                AVG(CASE WHEN has_media THEN view_count ELSE NULL END) as avg_views_with_media,
                AVG(CASE WHEN NOT has_media THEN view_count ELSE NULL END) as avg_views_without_media,
                SUM(CASE WHEN has_media THEN forward_count ELSE 0 END) as forwards_with_media,
                SUM(CASE WHEN NOT has_media THEN forward_count ELSE 0 END) as forwards_without_media
            FROM marts.fct_messages fm
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dd.full_date BETWEEN %s AND %s;
        """
        
        db.cursor.execute(overall_query, (start_date.date(), end_date.date()))
        overall_stats = db.cursor.fetchone()
        
        # Query 2: Channel-wise visual content
        channel_query = """
            SELECT 
                dc.channel_name,
                COUNT(*) as total_posts,
                SUM(CASE WHEN fm.has_media THEN 1 ELSE 0 END) as media_posts,
                ROUND((SUM(CASE WHEN fm.has_media THEN 1 ELSE 0 END)::float / COUNT(*)) * 100, 2) as media_percentage,
                AVG(CASE WHEN fm.has_media THEN fm.view_count ELSE NULL END) as avg_views_media,
                AVG(CASE WHEN NOT fm.has_media THEN fm.view_count ELSE NULL END) as avg_views_no_media
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dd.full_date BETWEEN %s AND %s
            GROUP BY dc.channel_name
            ORDER BY media_percentage DESC;
        """
        
        db.cursor.execute(channel_query, (start_date.date(), end_date.date()))
        channel_stats = db.cursor.fetchall()
        
        # Query 3: YOLO image analysis if available
        yolo_query = """
            SELECT 
                image_category,
                COUNT(*) as count,
                AVG(classification_confidence) as avg_confidence
            FROM external.yolo_detections
            GROUP BY image_category
            ORDER BY count DESC;
        """
        
        try:
            db.cursor.execute(yolo_query)
            yolo_stats = db.cursor.fetchall()
        except:
            yolo_stats = []  # YOLO table might not exist yet
        
        # Calculate engagement metrics
        total_messages = overall_stats[0] or 0
        messages_with_media = overall_stats[1] or 0
        messages_without_media = total_messages - messages_with_media
        
        avg_views_with_media = float(overall_stats[2]) if overall_stats[2] else 0.0
        avg_views_without_media = float(overall_stats[3]) if overall_stats[3] else 0.0
        
        # Calculate media effectiveness
        if avg_views_without_media > 0:
            media_effectiveness = ((avg_views_with_media - avg_views_without_media) / avg_views_without_media) * 100
        else:
            media_effectiveness = 0.0
        
        # Format response
        channel_data = []
        for row in channel_stats:
            channel_data.append({
                "channel_name": row[0],
                "total_posts": row[1],
                "media_posts": row[2],
                "media_percentage": float(row[3]) if row[3] else 0.0,
                "avg_views_with_media": float(row[4]) if row[4] else 0.0,
                "avg_views_without_media": float(row[5]) if row[5] else 0.0
            })
        
        yolo_data = []
        for row in yolo_stats:
            yolo_data.append({
                "category": row[0],
                "count": row[1],
                "avg_confidence": float(row[2]) if row[2] else 0.0
            })
        
        response = {
            "analysis_period": f"last_{days}_days",
            "total_messages_analyzed": total_messages,
            "messages_with_media": messages_with_media,
            "messages_without_media": messages_without_media,
            "media_percentage": (messages_with_media / total_messages * 100) if total_messages > 0 else 0.0,
            "avg_views_with_media": avg_views_with_media,
            "avg_views_without_media": avg_views_without_media,
            "media_effectiveness_percentage": round(media_effectiveness, 2),
            "forwards_with_media": overall_stats[4] or 0,
            "forwards_without_media": overall_stats[5] or 0,
            "channels": channel_data,
            "image_analysis": {
                "available": len(yolo_data) > 0,
                "categories": yolo_data
            },
            "insights": [
                f"Media posts get {round(media_effectiveness, 1)}% more views on average" if media_effectiveness > 0 else "Media posts don't significantly affect views",
                f"{(messages_with_media / total_messages * 100):.1f}% of all posts include media"
            ]
        }
        
        logger.info(f"Visual content analysis returned data for {len(channel_data)} channels")
        return response
        
    except Exception as e:
        logger.error(f"Error in visual-content endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Additional Endpoints
@app.get("/api/reports/product-trends/{product_category}",
         response_model=ProductTrendResponse,
         tags=["Analytics"])
async def get_product_trends(product_category: str, days: int = 30):
    """Get trending analysis for specific product category"""
    # Implementation would be similar to other endpoints
    pass

@app.get("/api/reports/channel-comparison",
         response_model=List[ChannelComparisonResponse],
         tags=["Analytics"])
async def compare_channels(channel1: str, channel2: str, days: int = 30):
    """Compare two channels across multiple metrics"""
    # Implementation would be similar to other endpoints
    pass

@app.get("/api/channels", tags=["Channels"])
async def list_channels():
    """List all available channels"""
    try:
        db.cursor.execute("""
            SELECT channel_name, channel_type, total_posts, avg_views, media_percentage
            FROM marts.dim_channels
            ORDER BY total_posts DESC;
        """)
        channels = db.cursor.fetchall()
        
        return [
            {
                "channel_name": row[0],
                "channel_type": row[1],
                "total_posts": row[2],
                "avg_views": float(row[3]) if row[3] else 0.0,
                "media_percentage": float(row[4]) if row[4] else 0.0
            }
            for row in channels
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)