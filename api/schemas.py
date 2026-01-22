"""
Pydantic models for FastAPI request/response validation
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime

# Request Models
class SearchRequest(BaseModel):
    query: str = Field(..., description="Search keyword")
    channel_name: Optional[str] = Field(None, description="Filter by channel")
    limit: int = Field(20, ge=1, le=100, description="Maximum results")
    offset: int = Field(0, ge=0, description="Pagination offset")

class DateRangeRequest(BaseModel):
    start_date: date = Field(..., description="Start date for analysis")
    end_date: date = Field(..., description="End date for analysis")
    channel_name: Optional[str] = Field(None, description="Filter by channel")

# Response Models
class TopProductsResponse(BaseModel):
    product_category: str = Field(..., description="Product category")
    mention_count: int = Field(..., description="Number of mentions")
    channel_count: int = Field(..., description="Number of channels mentioning")
    avg_views: float = Field(..., description="Average views per mention")
    media_count: int = Field(..., description="Number of mentions with media")
    media_percentage: float = Field(..., description="Percentage with media")
    trend: str = Field(..., description="Trend direction (increasing/stable/decreasing)")

class ChannelActivityItem(BaseModel):
    period: str = Field(..., description="Time period (date/week/month)")
    post_count: int = Field(..., description="Number of posts in period")
    total_views: int = Field(..., description="Total views in period")
    avg_views: float = Field(..., description="Average views per post")
    total_forwards: int = Field(..., description="Total forwards in period")
    media_posts: int = Field(..., description="Posts with media")
    unique_products: int = Field(..., description="Unique products mentioned")

class ChannelActivityResponse(BaseModel):
    channel_name: str = Field(..., description="Channel name")
    channel_type: str = Field(..., description="Channel type")
    analysis_period: str = Field(..., description="Analysis time period")
    period_granularity: str = Field(..., description="Period granularity")
    total_posts: int = Field(..., description="Total posts in period")
    avg_posts_per_period: float = Field(..., description="Average posts per period")
    trend_direction: str = Field(..., description="Posting trend direction")
    trend_percentage: float = Field(..., description="Trend percentage change")
    activity_data: List[ChannelActivityItem] = Field(..., description="Detailed activity data")

class MessageSearchResponse(BaseModel):
    message_id: int = Field(..., description="Message ID")
    channel_name: str = Field(..., description="Channel name")
    message_date: Optional[datetime] = Field(None, description="Message timestamp")
    message_preview: str = Field(..., description="Preview of message text")
    view_count: int = Field(..., description="Number of views")
    forward_count: int = Field(..., description="Number of forwards")
    has_media: bool = Field(..., description="Whether message has media")
    product_category: Optional[str] = Field(None, description="Product category")
    popularity_percentage: float = Field(..., description="Popularity percentage")
    relevance_score: float = Field(..., description="Search relevance score")

class ChannelVisualStats(BaseModel):
    channel_name: str = Field(..., description="Channel name")
    total_posts: int = Field(..., description="Total posts")
    media_posts: int = Field(..., description="Posts with media")
    media_percentage: float = Field(..., description="Percentage with media")
    avg_views_with_media: float = Field(..., description="Average views for media posts")
    avg_views_without_media: float = Field(..., description="Average views for non-media posts")

class ImageAnalysisCategory(BaseModel):
    category: str = Field(..., description="Image category")
    count: int = Field(..., description="Number of images")
    avg_confidence: float = Field(..., description="Average confidence score")

class VisualContentResponse(BaseModel):
    analysis_period: str = Field(..., description="Analysis time period")
    total_messages_analyzed: int = Field(..., description="Total messages analyzed")
    messages_with_media: int = Field(..., description="Messages with media")
    messages_without_media: int = Field(..., description="Messages without media")
    media_percentage: float = Field(..., description="Percentage with media")
    avg_views_with_media: float = Field(..., description="Average views with media")
    avg_views_without_media: float = Field(..., description="Average views without media")
    media_effectiveness_percentage: float = Field(..., description="Media effectiveness percentage")
    forwards_with_media: int = Field(..., description="Forwards from media posts")
    forwards_without_media: int = Field(..., description="Forwards from non-media posts")
    channels: List[ChannelVisualStats] = Field(..., description="Channel-wise statistics")
    image_analysis: Dict[str, Any] = Field(..., description="YOLO image analysis results")
    insights: List[str] = Field(..., description="Key insights")

class ProductTrendResponse(BaseModel):
    product_category: str = Field(..., description="Product category")
    analysis_period: str = Field(..., description="Analysis time period")
    total_mentions: int = Field(..., description="Total mentions")
    trend_data: List[Dict[str, Any]] = Field(..., description="Trend data over time")
    top_channels: List[str] = Field(..., description="Top channels mentioning product")
    price_range: Optional[Dict[str, float]] = Field(None, description="Price range observed")

class ChannelComparisonResponse(BaseModel):
    metric: str = Field(..., description="Comparison metric")
    channel1_value: Any = Field(..., description="Value for channel 1")
    channel2_value: Any = Field(..., description="Value for channel 2")
    difference_percentage: float = Field(..., description="Percentage difference")
    winner: Optional[str] = Field(None, description="Channel with better metric")

# Health Check Response
class HealthResponse(BaseModel):
    status: str = Field(..., description="Service status")
    timestamp: datetime = Field(..., description="Current timestamp")
    database: str = Field(..., description="Database status")
    version: str = Field(..., description="API version")