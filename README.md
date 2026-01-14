# Loop Fivetran Connector

Fivetran SDK connector for syncing Loop subscription data for [loopwork.co](loopwork.co)

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

## Notes

- The connector handles migration from multiple subscription products to a unified product
- Supports full refresh and incremental syncs
- Uses composite IDs matching the Lambda implementation for consistency
