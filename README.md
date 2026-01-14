# Loop Fivetran Connector

Fivetran SDK connector for syncing [Loop](https://loopwork.co) subscription data.



## Overview

Extracts data from Loop API including:

- Subscriptions
- Subscription Line Items
- Orders
- Activities

## Setup

1. Install dependencies:

```bash
pip install fivetran-connector-sdk requests
```

1. Create `configuration.json`:

```json
{
  "LOOP_API_TOKEN": "your-api-token",
  "LOOP_API_URL": "https://api.loopsubscriptions.com/admin/2023-10",
  "LOOP_SYNC_WINDOW_DAYS": "7"
}
```

## Testing

Run locally with:

```bash
fivetran debug --configuration configuration.json
```

This creates a local `warehouse.db` file with synced data.

## Configuration

- `LOOP_API_TOKEN`: Your Loop API authentication token (required)
- `LOOP_API_URL`: Loop API base URL (default: `https://api.loopsubscriptions.com/admin/2023-10`)
- `LOOP_SYNC_WINDOW_DAYS`: Sync window in days for incremental syncs (default: `7`)

## Table Schema

### subscriptions
**Primary Key:** `id`

- `id` (string) - Composite ID: `{shopify_id}:{updated_at_timestamp}`
- `loop_id` (integer)
- `subscription_shopify_id` (integer)
- `origin_order_shopify_id` (integer)
- `customer_shopify_id` (integer)
- `status` (string)
- `created_at` (timestamp)
- `updated_at` (timestamp)
- `cancelled_at` (timestamp)
- `paused_at` (timestamp)
- `next_billing_epoch` (integer)
- `next_billing_date` (timestamp)
- `last_payment_status` (string)
- `currency_code` (string)
- `total_line_item_price` (decimal)
- `total_line_item_discounted_price` (decimal)
- `delivery_price` (decimal)
- `completed_order_count` (integer)
- `cancellation_reason` (string)
- `cancellation_comment` (string)
- `is_prepaid` (boolean)
- `billing_interval` (string)
- `billing_interval_count` (integer)
- `billing_min_cycles` (integer)
- `billing_max_cycles` (integer)
- `billing_anchor_type` (string)
- `billing_anchor_day` (integer)
- `billing_anchor_month` (string)
- `delivery_interval` (string)
- `delivery_interval_count` (integer)

### subscription_line_items
**Primary Key:** `id`

- `id` (string) - Composite ID: `{line_item_id}:{updated_at_timestamp}`
- `line_id` (integer)
- `subscription_id` (integer) - Shopify subscription ID
- `variant_shopify_id` (integer)
- `product_shopify_id` (integer)
- `variant_title` (string)
- `product_title` (string)
- `name` (string)
- `quantity` (integer)
- `price` (decimal)
- `base_price` (decimal)
- `discounted_price` (decimal)
- `sku` (string)
- `is_one_time_added` (boolean)
- `is_one_time_removed` (boolean)
- `bundle_transaction_id` (integer)
- `attributes` (json)

### orders
**Primary Key:** `id`

- `id` (string) - Composite ID: `{order_id}:{shopify_updated_at_timestamp}`
- `loop_id` (integer)
- `order_shopify_id` (integer)
- `subscription_id` (integer)
- `subscription_shopify_id` (integer)
- `customer_shopify_id` (integer)
- `status` (string)
- `financial_status` (string)
- `billing_date_epoch` (integer)
- `shopify_created_at` (timestamp)
- `shopify_processed_at` (timestamp)
- `shopify_updated_at` (timestamp)
- `currency_code` (string)
- `total_price_usd` (decimal)
- `total_discount` (decimal)
- `total_line_items_price` (decimal)
- `total_shipping_price` (decimal)
- `shopify_order_number` (integer)

### activities
**Primary Key:** `id`

- `id` (string) - Activity ID
- `subscription_id` (integer)
- `order_id` (integer) - Extracted from metadata
- `event` (string)
- `entity` (string)
- `source` (string)
- `created_at` (timestamp)
- `data` (json)

## Notes

- The connector handles migration from multiple subscription products to a unified product
- Supports full refresh and incremental syncs
- Uses composite IDs matching the Lambda implementation for consistency
