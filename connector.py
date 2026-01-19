"""
Fivetran-native SDK connector for Loop subscriptions.

This connector extracts data from Loop API including:
- Subscriptions
- Orders
- Activities

It uses the Fivetran Connector SDK to sync data incrementally.
"""

import datetime
import json
import time
from typing import Any, Optional

import requests

from fivetran_connector_sdk import Connector, Logging as log, Operations as op  # type: ignore[import-untyped]

# Global variables to store configuration (set by get_configuration)
_loop_client: Optional["LoopAPIClient"] = None
_sync_window_days: int = 7


class LoopAPIClient:
    """
    Simple Loop API client for making requests to Loop API.
    """

    def __init__(self, api_token: str, api_url: str) -> None:
        """
        Initialize the Loop API client.

        Args:
            api_token: Loop API authentication token
            api_url: Loop API base URL
        """
        self.api_token = api_token
        self.api_url = api_url.rstrip("/")

    def headers(self) -> dict[str, str]:
        """
        Return the headers to use for requests to the Loop API.

        Returns:
            Headers dictionary
        """
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "X-Loop-Token": self.api_token,
        }

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        """
        Make a GET request to the Loop API with retry logic and rate limit handling.

        Args:
            url: Full URL or path relative to base URL
            **kwargs: Additional arguments to pass to requests.get

        Returns:
            Response object

        Raises:
            requests.RequestException: If all retry attempts fail
        """
        if not url.startswith("http"):
            url = f"{self.api_url}/{url.lstrip('/')}"

        if "headers" not in kwargs:
            kwargs["headers"] = self.headers()

        # Retry logic: 5 attempts with exponential backoff
        max_retries = 5
        retryable_statuses = {429, 500, 502, 503, 504}

        # Set default timeout if not provided
        if "timeout" not in kwargs:
            kwargs["timeout"] = 30

        # Extract page number from URL for better logging (if available)
        page_info = ""
        if "pageNo=" in url:
            try:
                page_num = url.split("pageNo=")[1].split("&")[0]
                page_info = f" (page {page_num})"
            except:
                pass

        rate_limited = False
        for attempt in range(max_retries):
            try:
                response = requests.get(url, **kwargs)

                # If successful or non-retryable error, return immediately
                if response.status_code not in retryable_statuses:
                    # If we had to retry due to rate limits, add extra delay after success, this helps prevent immediately hitting the limit again
                    if rate_limited and attempt > 0:
                        extra_delay = 2.0  # Add 2 seconds after recovering from rate limit
                        log.info(f"Recovered from rate limit{page_info}. Waiting {extra_delay}s before continuing...")
                        time.sleep(extra_delay)
                    
                    # Check for rate limit headers even on successful responses
                    # Loop API returns X-RateLimit-Remaining header
                    rate_limit_remaining = response.headers.get("X-RateLimit-Remaining")
                    if rate_limit_remaining:
                        try:
                            remaining = int(rate_limit_remaining)
                            # Adaptive rate limiting based on remaining requests
                            if remaining == 0:
                                # No remaining requests - wait longer to let bucket refill
                                log.warning(f"Rate limit remaining: 0. Waiting 3s for bucket to refill...")
                                time.sleep(3.0)
                            elif remaining <= 1:
                                # Very low - wait a bit
                                log.warning(f"Rate limit remaining: {remaining}. Adding 2s delay.")
                                time.sleep(2.0)
                            elif remaining <= 3:
                                # Low - add small delay
                                log.info(f"Rate limit remaining: {remaining}. Adding 1s delay.")
                                time.sleep(1.0)
                            # If remaining > 3, no extra delay needed
                        except (ValueError, TypeError):
                            pass
                    return response

                # Handle 429 rate limit with Retry-After header support
                if response.status_code == 429:
                    rate_limited = True
                    # Check for Retry-After header (in seconds)
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = int(retry_after)
                            # Add a small buffer to the Retry-After time to be safe
                            wait_time = max(wait_time, 2)  # Minimum 2 seconds
                            log.warning(f"Rate limited{page_info}. Waiting {wait_time} seconds as specified by Retry-After header")
                        except (ValueError, TypeError):
                            # If Retry-After is invalid, use exponential backoff
                            wait_time = min(2 ** (attempt + 2), 60)  # Cap at 60 seconds
                    else:
                        # No Retry-After header, use longer exponential backoff for rate limits
                        wait_time = min(2 ** (attempt + 2), 60)  # Cap at 60 seconds
                    
                    # If last attempt, raise the error
                    if attempt == max_retries - 1:
                        log.severe(f"Rate limit exceeded{page_info} after {max_retries} attempts. Last wait time: {wait_time}s")
                        response.raise_for_status()
                        return response
                    
                    log.warning(f"Rate limited{page_info} - attempt {attempt + 1}/{max_retries}. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue

                # For other retryable errors (5xx), use standard exponential backoff
                # If last attempt, raise the error
                if attempt == max_retries - 1:
                    response.raise_for_status()
                    return response

                # Exponential backoff: wait 2^attempt seconds
                wait_time = 2**attempt
                time.sleep(wait_time)

            except requests.RequestException as e:
                # If last attempt, re-raise the exception
                if attempt == max_retries - 1:
                    raise

                # Exponential backoff: wait 2^attempt seconds
                wait_time = 2**attempt
                log.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)

        # Should never reach here, but just in case
        return requests.get(url, **kwargs)


def validate_configuration(configuration: dict[str, Any]) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    
    Args:
        configuration: Dictionary holding configuration settings
        
    Raises:
        ValueError: If any required configuration parameter is missing
    """
    api_token = str(
        configuration.get("LOOP_API_TOKEN") or configuration.get("loop_api_token") or ""
    )
    if not api_token or api_token in ["your-loop-api-token-here", "test-token-123"]:
        # For testing, allow placeholder tokens but warn
        if api_token in ["your-loop-api-token-here", "test-token-123"]:
            log.warning("Using placeholder API token. Real API calls will fail.")
        else:
            raise ValueError("LOOP_API_TOKEN is required in configuration")


def _initialize_client(configuration: dict[str, Any]) -> None:
    """
    Initialize the Loop API client from configuration.
    This is called from update() to ensure client is initialized.

    Args:
        configuration: Configuration dictionary from SDK
    """
    global _loop_client, _sync_window_days

    if _loop_client is not None:
        return

    # Validate configuration first
    validate_configuration(configuration)

    # Configuration is flat, not nested under "secrets"
    # Get required configuration
    api_token = str(
        configuration.get("LOOP_API_TOKEN") or configuration.get("loop_api_token") or ""
    )

    api_url = str(
        configuration.get("LOOP_API_URL")
        or configuration.get("loop_api_url")
        or "https://api.loopsubscriptions.com/admin/2023-10"
    )

    # Initialize Loop API client
    _loop_client = LoopAPIClient(api_token, api_url)

    # Default sync window (7 days)
    sync_window_str = str(
        configuration.get("LOOP_SYNC_WINDOW_DAYS")
        or configuration.get("loop_sync_window_days")
        or "7"
    )
    _sync_window_days = int(sync_window_str)


def schema(configuration: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Define the schema for Loop data tables.

    Args:
        configuration: Configuration dictionary (provided by SDK)

    Returns:
        List of table definitions, each with 'table' and 'primary_key' fields
    """
    return [
        {"table": "subscriptions", "primary_key": ["id"]},
        {"table": "subscription_line_items", "primary_key": ["id"]},
        {"table": "orders", "primary_key": ["id"]},
        {"table": "activities", "primary_key": ["id"]},
    ]


def update(configuration: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    """
    Main sync method that extracts data from Loop API and writes to Fivetran.

    Args:
        configuration: Configuration dictionary (provided by SDK)
        state: Current state dictionary for incremental syncs

    Returns:
        Updated state dictionary
    """
    log.info("Starting Loop connector sync")
    global _loop_client, _sync_window_days

    # Validate configuration
    validate_configuration(configuration)

    # Initialize client if not already done
    _initialize_client(configuration)

    # Detect full refresh: sync_id mismatch or empty state (UpdatedSince == 0)
    # Note: Fivetran SDK handles full refresh differently, but we check for empty state
    sync_id = state.get("sync_id", "")
    request_sync_id = configuration.get("sync_id", "")
    updated_since = state.get("updated_since", 0)

    is_full_refresh = (
        sync_id != request_sync_id
        or updated_since == 0
        or not state.get("last_sync_timestamp")
    )

    # Calculate start date
    # Check for test mode: limit date range for testing (useful for debug mode)
    test_days_back = configuration.get("TEST_DAYS_BACK")
    if test_days_back:
        # Test mode: only sync last N days
        try:
            test_days = int(test_days_back)
            start_date = datetime.datetime.now(
                datetime.timezone.utc
            ) - datetime.timedelta(days=test_days)
            log.info(f"TEST MODE: Limiting sync to last {test_days} days")
        except (ValueError, TypeError):
            # Invalid test_days_back, use normal logic
            test_days_back = None
    
    if not test_days_back:
        if is_full_refresh:
            # For full refresh, use epoch 0 to fetch all records
            start_date = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
        else:
            # For incremental sync, use the state's UpdatedSince or sync_window_days ago
            if updated_since > 0:
                start_date = datetime.datetime.fromtimestamp(
                    updated_since, tz=datetime.timezone.utc
                )
            else:
                start_date = datetime.datetime.now(
                    datetime.timezone.utc
                ) - datetime.timedelta(days=_sync_window_days)

    # End date: Lambda uses current time + 24 hours to ensure we get all records
    end_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        hours=24
    )
    start_epoch = int(start_date.timestamp())
    end_epoch = int(end_date.timestamp())

    try:
        # Sync subscriptions
        log.info(f"Syncing subscriptions from {start_date.isoformat()} to {end_date.isoformat()}")
        _sync_subscriptions(start_epoch, end_epoch)

        # Sync orders
        log.info(f"Syncing orders from {start_date.isoformat()} to {end_date.isoformat()}")
        _sync_orders(start_epoch, end_epoch)

        # Sync activities
        log.info(f"Syncing activities from {start_date.isoformat()} to {end_date.isoformat()}")
        _sync_activities(start_epoch, end_epoch)

        # Update state with current timestamp and sync_id
        updated_state = {
            "last_sync_timestamp": end_date.isoformat(),
            "updated_since": int(start_date.timestamp()),
            "sync_id": request_sync_id if request_sync_id else sync_id,
        }
        op.checkpoint(updated_state)
        log.info("Sync completed successfully")
        return updated_state
    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync Loop data: {str(e)}")


def _sync_subscriptions(start_epoch: int, end_epoch: int) -> None:
    """
    Sync subscriptions from Loop API.

    Args:
        start_epoch: Start timestamp in epoch seconds
        end_epoch: End timestamp in epoch seconds
    """
    global _loop_client

    page = 1
    has_more = True
    consecutive_failures = 0
    max_consecutive_failures = 3

    while has_more:
        # Call Loop API subscriptions endpoint with date filtering
        subscriptions = _get_subscriptions_page(page, start_epoch, end_epoch)

        # If we get an empty response, check if it's a failure or truly no more data
        if not subscriptions:
            # Check if we had a previous successful page to determine if there's more data
            # If this is page 1 and it fails, we should stop
            if page == 1:
                log.warning("No data returned on first page. Stopping subscription sync.")
                break
            
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                log.warning(f"Too many consecutive failures ({consecutive_failures}). Stopping subscription sync.")
                break
            
            # Wait a bit before retrying the same page
            log.info(f"Empty response on page {page}. Waiting before continuing...")
            time.sleep(5)
            continue
        
        # Reset failure counter on success
        consecutive_failures = 0

        subscriptions_data = []
        subscription_line_items_data = []

        for sub in subscriptions.get("data", []):
            # Extract subscription data
            customer = sub.get("customer", {})
            shopify_id = sub.get("shopifyId", 0)
            updated_at = sub.get("updatedAt")

            # Generate composite ID matching Lambda: "{shopify_id}:{updated_at_timestamp}"
            # Ensure shopify_id is always a valid number
            if not shopify_id or shopify_id == 0:
                continue  # Skip records without valid shopify_id
            updated_at_epoch = _get_timestamp_epoch(updated_at)
            subscription_id = f"{shopify_id}:{updated_at_epoch}"

            # Extract billing and delivery policies
            billing_policy = sub.get("billingPolicy", {})
            delivery_policy = sub.get("deliveryPolicy", {})

            subscription_record = {
                "id": str(subscription_id),  # Ensure ID is always a string
                "loop_id": sub.get("id", 0),
                "subscription_shopify_id": shopify_id,  # Note: Lambda uses "SubcriptionShopifyID" (typo preserved)
                "origin_order_shopify_id": sub.get("originOrderShopifyId", 0),
                "customer_shopify_id": customer.get("shopifyId", 0),
                "status": sub.get("status", ""),
                "created_at": _parse_timestamp(sub.get("createdAt")),
                "updated_at": _parse_timestamp(updated_at),
                "cancelled_at": _parse_timestamp(sub.get("cancelledAt")),
                "paused_at": _parse_timestamp(sub.get("pausedAt")),
                "next_billing_epoch": sub.get("nextBillingDateEpoch"),
                "next_billing_date": _parse_timestamp_from_epoch(
                    sub.get("nextBillingDateEpoch")
                ),
                "last_payment_status": sub.get("lastPaymentStatus", ""),
                "currency_code": sub.get("currencyCode", ""),
                "total_line_item_price": _parse_decimal(sub.get("totalLineItemPrice")),
                "total_line_item_discounted_price": _parse_decimal(
                    sub.get("totalLineItemDiscountedPrice")
                ),
                "delivery_price": _parse_decimal(sub.get("deliveryPrice")),
                "completed_order_count": sub.get("completedOrdersCount", 0),
                "cancellation_reason": sub.get("cancellationReason"),
                "cancellation_comment": sub.get("cancellationComment"),
                "is_prepaid": sub.get("isPrepaid", False),
                # Billing policy fields
                "billing_interval": billing_policy.get("interval"),
                "billing_interval_count": billing_policy.get("intervalCount"),
                "billing_min_cycles": billing_policy.get("minCycles"),
                "billing_max_cycles": billing_policy.get("maxCycles"),
                "billing_anchor_type": billing_policy.get("anchorType"),
                "billing_anchor_day": billing_policy.get("anchorDay"),
                "billing_anchor_month": billing_policy.get("anchorMonth"),
                # Delivery policy fields
                "delivery_interval": delivery_policy.get("interval"),
                "delivery_interval_count": delivery_policy.get("intervalCount"),
            }
            subscriptions_data.append(subscription_record)

            # Extract line items
            for line_item in sub.get("lines", []):
                line_item_id = line_item.get("id", 0)
                # Skip if line_item_id is invalid
                if not line_item_id or line_item_id == 0:
                    continue
                # Generate composite ID matching Lambda: "{line_item_id}:{updated_at_timestamp}"
                line_item_record_id = f"{line_item_id}:{updated_at_epoch}"

                line_item_record = {
                    "id": str(line_item_record_id),  # Ensure ID is always a string
                    "line_id": line_item_id,
                    "subscription_id": shopify_id,  # Lambda uses subscription_shopify_id (subscription_sid)
                    "variant_shopify_id": line_item.get("variantShopifyId", 0),
                    "product_shopify_id": line_item.get("productShopifyId", 0),
                    "variant_title": line_item.get("variantTitle", ""),
                    "product_title": line_item.get("productTitle", ""),
                    "name": line_item.get("name", ""),
                    "quantity": line_item.get("quantity", 0),
                    "price": _parse_decimal(line_item.get("price")),
                    "base_price": _parse_decimal(line_item.get("basePrice")),
                    "discounted_price": _parse_decimal(
                        line_item.get("discountedPrice")
                    ),
                    "sku": line_item.get("sku", ""),
                    "is_one_time_added": line_item.get("isOneTimeAdded", False),
                    "is_one_time_removed": line_item.get("isOneTimeRemoved", False),
                    "bundle_transaction_id": line_item.get("bundleTransactionId"),
                    "attributes": _to_json(line_item.get("attributes", [])),
                }
                subscription_line_items_data.append(line_item_record)

        # Write subscriptions in batch
        # Filter out any records with invalid IDs before inserting
        valid_subscriptions = [
            s
            for s in subscriptions_data
            if s.get("id") and str(s.get("id")).strip() != ""
        ]
        if valid_subscriptions:
            # Fivetran SDK expects data as list of records, not wrapped in {"insert": ...}
            for record in valid_subscriptions:
                op.upsert(table="subscriptions", data=record)

        # Write line items in batch
        # Filter out any records with invalid IDs before inserting
        valid_line_items = [
            li
            for li in subscription_line_items_data
            if li.get("id") and str(li.get("id")).strip() != ""
        ]
        if valid_line_items:
            # Fivetran SDK expects data as list of records, not wrapped in {"insert": ...}
            for record in valid_line_items:
                op.upsert(table="subscription_line_items", data=record)

        # Check if there are more pages
        has_more = subscriptions.get("pageInfo", {}).get("hasNextPage", False)
        page += 1
        
        # Add a delay between pages to avoid hitting rate limits
        # Loop API has strict rate limits (~2 requests per 2 seconds)
        if has_more:
            time.sleep(2.5)  # 2.5 second delay between pages to respect rate limits


def _sync_orders(start_epoch: int, end_epoch: int) -> None:
    """
    Sync orders from Loop API.

    Args:
        start_epoch: Start timestamp in epoch seconds
        end_epoch: End timestamp in epoch seconds
    """
    page = 1
    has_more = True
    consecutive_failures = 0
    max_consecutive_failures = 3

    while has_more:
        # Call Loop API orders endpoint with date filtering
        orders_response = _get_orders_page(page, start_epoch, end_epoch)

        if not orders_response:
            if page == 1:
                log.warning("No data returned on first page. Stopping orders sync.")
                break
            
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                log.warning(f"Too many consecutive failures ({consecutive_failures}). Stopping orders sync.")
                break
            
            log.info(f"Empty response on page {page}. Waiting before continuing...")
            time.sleep(5)
            continue
        
        consecutive_failures = 0

        orders_data = []

        for order in orders_response.get("data", []):
            subscription = order.get("subscription", {})
            customer = order.get("customer", {})
            order_id = order.get("id", 0)
            shopify_updated_at = order.get("shopifyUpdatedAt")

            # Skip if order_id is invalid
            if not order_id or order_id == 0:
                continue
            # Generate composite ID matching Lambda: "{order_id}:{shopify_updated_at_timestamp}"
            updated_at_epoch = _get_timestamp_epoch(shopify_updated_at)
            order_record_id = f"{order_id}:{updated_at_epoch}"

            order_record = {
                "id": str(order_record_id),  # Ensure ID is always a string
                "loop_id": order_id,
                "order_shopify_id": order.get("shopifyId"),
                "subscription_id": subscription.get("id", 0),
                "subscription_shopify_id": subscription.get("shopifyId", 0),
                "customer_shopify_id": customer.get("shopifyId", 0),
                "status": order.get("status", ""),
                "financial_status": order.get("financialStatus", ""),
                "billing_date_epoch": order.get("billingDateEpoch"),
                "shopify_created_at": _parse_timestamp(order.get("shopifyCreatedAt")),
                "shopify_processed_at": _parse_timestamp(
                    order.get("shopifyProcessedAt")
                ),
                "shopify_updated_at": _parse_timestamp(shopify_updated_at),
                "currency_code": order.get("currencyCode", ""),
                "total_price_usd": _parse_decimal(order.get("totalPriceUsd")),
                "total_discount": _parse_decimal(order.get("totalDiscount")),
                "total_line_items_price": _parse_decimal(
                    order.get("totalLineItemsPrice")
                ),
                "total_shipping_price": _parse_decimal(order.get("totalShippingPrice")),
                "shopify_order_number": order.get("shopifyOrderNumber", 0),
            }
            orders_data.append(order_record)

        # Filter out any records with invalid IDs before inserting
        valid_orders = [
            o for o in orders_data if o.get("id") and str(o.get("id")).strip() != ""
        ]
        if valid_orders:
            # Fivetran SDK expects data as record, not wrapped in {"insert": ...}
            for record in valid_orders:
                op.upsert(table="orders", data=record)

        has_more = orders_response.get("pageInfo", {}).get("hasNextPage", False)
        page += 1
        
        # Add a delay between pages to avoid hitting rate limits
        if has_more:
            time.sleep(2.5)  # 2.5 second delay between pages to respect rate limits


def _sync_activities(start_epoch: int, end_epoch: int) -> None:
    """
    Sync activities from Loop API.

    Args:
        start_epoch: Start timestamp in epoch seconds
        end_epoch: End timestamp in epoch seconds
    """
    page = 1
    has_more = True
    consecutive_failures = 0
    max_consecutive_failures = 3

    while has_more:
        # Call Loop API activities endpoint with date filtering
        activities_response = _get_activities_page(page, start_epoch, end_epoch)

        if not activities_response:
            if page == 1:
                log.warning("No data returned on first page. Stopping activities sync.")
                break
            
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                log.warning(f"Too many consecutive failures ({consecutive_failures}). Stopping activities sync.")
                break
            
            log.info(f"Empty response on page {page}. Waiting before continuing...")
            time.sleep(5)
            continue
        
        consecutive_failures = 0

        activities_data = []
        for activity in activities_response.get("data", []):
            activity_id = activity.get("id", 0)

            # Skip if activity_id is invalid
            if not activity_id or activity_id == 0:
                continue

            subscription_id = activity.get("subscriptionId")

            # Lambda uses activity ID directly (int64), not composite
            # But we'll keep it as string for consistency with other IDs

            # Extract order ID from metadata if available
            order_id = None
            data = activity.get("data", {})
            if isinstance(data, dict):
                order_shopify_id = data.get("orderShopifyId")
                if order_shopify_id is not None:
                    try:
                        order_id = int(float(order_shopify_id))
                    except (ValueError, TypeError):
                        pass

            activity_record = {
                "id": str(activity_id),  # Lambda uses int64 directly, convert to string
                "subscription_id": subscription_id,
                "order_id": order_id,  # Extracted from metadata
                "event": activity.get("key", ""),  # Lambda uses "Event" field
                "entity": activity.get("entity", ""),
                "source": activity.get("source", ""),
                "created_at": _parse_timestamp(activity.get("createdAt")),
                "data": _to_json(data),
            }
            activities_data.append(activity_record)

        # Filter out any records with invalid IDs before inserting
        valid_activities = [
            a for a in activities_data if a.get("id") and str(a.get("id")).strip() != ""
        ]
        if valid_activities:
            # Fivetran SDK expects data as record, not wrapped in {"insert": ...}
            for record in valid_activities:
                op.upsert(table="activities", data=record)

        has_more = activities_response.get("pageInfo", {}).get("hasNextPage", False)
        page += 1
        
        # Add a delay between pages to avoid hitting rate limits
        if has_more:
            time.sleep(2.5)  # 2.5 second delay between pages to respect rate limits


def _parse_timestamp(timestamp_str: Optional[str]) -> Optional[str]:
    """
    Parse timestamp string to ISO format.

    Args:
        timestamp_str: Timestamp string from Loop API

    Returns:
        ISO format timestamp string or None
    """
    if not timestamp_str:
        return None

    try:
        # Handle various timestamp formats
        if isinstance(timestamp_str, str) and "T" in timestamp_str:
            # ISO format: 2024-01-15T15:00:00.000Z
            dt = datetime.datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        else:
            # Epoch timestamp (as string or int)
            dt = datetime.datetime.fromtimestamp(
                int(timestamp_str), tz=datetime.timezone.utc
            )

        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def _parse_decimal(value: Optional[Any]) -> Optional[str]:
    """
    Parse decimal value to string.

    Args:
        value: Decimal value from Loop API

    Returns:
        String representation of decimal or None
    """
    if value is None:
        return None

    try:
        return str(float(value))
    except (ValueError, TypeError):
        return None


def _to_json(value: Any) -> Optional[str]:
    """
    Convert value to JSON string.

    Args:
        value: Value to convert

    Returns:
        JSON string or None
    """
    if value is None:
        return None

    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return None


def _get_api_page(
    endpoint: str,
    page: int,
    start_epoch: int,
    end_epoch: int,
    date_param_prefix: str = "updatedAt",
) -> dict[str, Any]:
    """
    Get a page of data from Loop API.

    Args:
        endpoint: API endpoint name (e.g., "subscription", "order", "activityLog")
        page: Page number
        start_epoch: Start timestamp in epoch seconds
        end_epoch: End timestamp in epoch seconds
        date_param_prefix: Prefix for date parameters ("updatedAt" or "createdAt")

    Returns:
        Response dictionary with data and pageInfo, or empty dict on failure
    """
    global _loop_client

    if _loop_client is None:
        return {}

    try:
        url = f"{endpoint}?pageNo={page}&pageSize=1000&{date_param_prefix}StartEpoch={start_epoch}&{date_param_prefix}EndEpoch={end_epoch}"
        log.info(f"Fetching {endpoint} page {page}...")
        response = _loop_client.get(url, timeout=30)
        response.raise_for_status()
        result: dict[str, Any] = response.json()  # type: ignore[assignment]
        log.info(f"Successfully fetched {endpoint} page {page}")
        return result
    except requests.HTTPError as e:
        # For HTTP errors (like 429), log and return empty to allow continuation
        if e.response and e.response.status_code == 429:
            log.warning(f"Rate limit hit on {endpoint} page {page} after retries. Will retry on next sync.")
        else:
            log.warning(f"HTTP error fetching {endpoint} page {page}: {e}")
        return {}
    except Exception as e:
        # Log error but don't fail the entire sync
        log.warning(f"Error fetching {endpoint} page {page}: {e}")
        return {}


def _get_subscriptions_page(
    page: int, start_epoch: int, end_epoch: int
) -> dict[str, Any]:
    """
    Get a page of subscriptions from Loop API.

    Args:
        page: Page number
        start_epoch: Start timestamp in epoch seconds
        end_epoch: End timestamp in epoch seconds

    Returns:
        Response dictionary with data and pageInfo
    """
    return _get_api_page("subscription", page, start_epoch, end_epoch, "updatedAt")


def _get_orders_page(page: int, start_epoch: int, end_epoch: int) -> dict[str, Any]:
    """
    Get a page of orders from Loop API.

    Args:
        page: Page number
        start_epoch: Start timestamp in epoch seconds
        end_epoch: End timestamp in epoch seconds

    Returns:
        Response dictionary with data and pageInfo
    """
    return _get_api_page("order", page, start_epoch, end_epoch, "updatedAt")


def _get_activities_page(page: int, start_epoch: int, end_epoch: int) -> dict[str, Any]:
    """
    Get a page of activities from Loop API.

    Args:
        page: Page number
        start_epoch: Start timestamp in epoch seconds
        end_epoch: End timestamp in epoch seconds

    Returns:
        Response dictionary with data and pageInfo
    """
    return _get_api_page("activityLog", page, start_epoch, end_epoch, "createdAt")


def _parse_timestamp_from_epoch(epoch: Optional[int]) -> Optional[str]:
    """
    Parse epoch timestamp to ISO format.

    Args:
        epoch: Epoch timestamp in seconds

    Returns:
        ISO format timestamp string or None
    """
    if epoch is None:
        return None

    try:
        dt = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc)
        return dt.isoformat()
    except (ValueError, TypeError, OSError):
        return None


def _get_timestamp_epoch(timestamp_str: Optional[str]) -> int:
    """
    Get epoch timestamp from timestamp string.
    Used for generating composite IDs matching Lambda format.

    Args:
        timestamp_str: Timestamp string from Loop API

    Returns:
        Epoch timestamp in seconds, or 0 if parsing fails
    """
    if not timestamp_str:
        return 0

    try:
        if isinstance(timestamp_str, str) and "T" in timestamp_str:
            # ISO format: 2024-01-15T15:00:00.000Z
            dt = datetime.datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            return int(dt.timestamp())
        else:
            # Already an epoch timestamp
            return int(float(timestamp_str))
    except (ValueError, TypeError):
        return 0


# Initialize connector - this is what Fivetran SDK looks for
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("/configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
