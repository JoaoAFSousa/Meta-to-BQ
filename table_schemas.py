import pandera as pa

# Define schema for campaigns table
campaigns_schema = pa.DataFrameSchema({
    "account_id": pa.Column(pa.String),
    "id": pa.Column(pa.String),
    "name": pa.Column(pa.String),
    "status": pa.Column(pa.String),
    "created_time": pa.Column(pa.DateTime),
    "updated_time": pa.Column(pa.DateTime),
    "objective": pa.Column(pa.String),
    "source_campaign_id": pa.Column(pa.String, nullable=True),
    "boosted_object_id": pa.Column(pa.String, nullable=True)
}, strict='filter', coerce=True, add_missing_columns=True)

# Define schema for adsets table
adsets_schema = pa.DataFrameSchema({
    "account_id": pa.Column(pa.String),
    "created_time": pa.Column(pa.DateTime),
    "end_time": pa.Column(pa.DateTime, nullable=True),
    "id": pa.Column(pa.String),
    "name": pa.Column(pa.String),
    "status": pa.Column(pa.String),
    "campaign_id": pa.Column(pa.String),
    "billing_event": pa.Column(pa.String),
    "daily_budget": pa.Column(pa.Float),
    "destination_type": pa.Column(pa.String),
    "optimization_goal": pa.Column(pa.String),
    "source_adset_id": pa.Column(pa.String, nullable=True),
    "promoted_object_pixel_id": pa.Column(pa.String, nullable=True),
    "promoted_object_custom_event_type": pa.Column(pa.String, nullable=True)
}, strict='filter', coerce=True, add_missing_columns=True)

# Define schema for ads table
ads_schema = pa.DataFrameSchema({
    "account_id": pa.Column(pa.String),
    "created_time": pa.Column(pa.DateTime),
    "id": pa.Column(pa.String),
    "adset_id": pa.Column(pa.String),
    "campaign_id": pa.Column(pa.String),
    "status": pa.Column(pa.String),
    "name": pa.Column(pa.String),
    "ad_active_time": pa.Column(pa.Int, nullable=False, default=0),
    "source_ad_id": pa.Column(pa.String, nullable=True),
    "preview_shareable_link": pa.Column(pa.String, nullable=True),
    "creative_id": pa.Column(pa.String)
}, strict='filter', coerce=True, add_missing_columns=True)

# Define schema for insights_ads table
insights_ads_schema = pa.DataFrameSchema({
    "date": pa.Column(pa.DateTime),
    "account_id": pa.Column(pa.String),
    "account_name": pa.Column(pa.String),
    "ad_id": pa.Column(pa.String),
    "ad_name": pa.Column(pa.String),
    "objective": pa.Column(pa.String),
    "optimization_goal": pa.Column(pa.String),
    "impressions": pa.Column(pa.Int, nullable=False, default=0),
    "reach": pa.Column(pa.Int, nullable=False, default=0),
    "spend": pa.Column(pa.Float),
    "action_page_engagement": pa.Column(pa.Int, nullable=False, default=0),
    "action_post_engagement": pa.Column(pa.Int, nullable=False, default=0),
    "action_video_view": pa.Column(pa.Int, nullable=False, default=0),
    "action_post_reaction": pa.Column(pa.Int, nullable=False, default=0),
    "action_like": pa.Column(pa.Int, nullable=False, default=0),
    "action_link_click": pa.Column(pa.Int, nullable=False, default=0),
    "action_landing_page_view": pa.Column(pa.Int, nullable=False, default=0),
    "action_lead": pa.Column(pa.Int, nullable=False, default=0),
    "action_onsite_conversion_messaging_conversation_started_7d": pa.Column(pa.Int, nullable=False, default=0),
    "action_offsite_conversion_fb_pixel_initiate_checkout": pa.Column(pa.Int, nullable=False, default=0),
    "action_offsite_conversion_fb_pixel_purchase": pa.Column(pa.Int, nullable=False, default=0),
    "video_p25_watched_actions": pa.Column(pa.Int, nullable=False, default=0),
    "video_p50_watched_actions": pa.Column(pa.Int, nullable=False, default=0),
    "video_p75_watched_actions": pa.Column(pa.Int, nullable=False, default=0),
    "video_p95_watched_actions": pa.Column(pa.Int, nullable=False, default=0),
    "video_p100_watched_actions": pa.Column(pa.Int, nullable=False, default=0)
}, strict='filter', coerce=True, add_missing_columns=True)
