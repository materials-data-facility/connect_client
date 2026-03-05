"""Globus authentication for MDF Agent.

This module provides authentication utilities for connecting to MDF Connect
and related Globus services. It supports both token-based authentication
(for automation) and interactive OAuth2 login (for human users).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from globus_sdk import (
    AccessTokenAuthorizer,
    GlobusAppConfig,
    NativeAppAuthClient,
    RefreshTokenAuthorizer,
    UserApp,
)
from globus_sdk.tokenstorage import JSONTokenStorage

# =============================================================================
# MDF Agent Credentials
# =============================================================================

# Native app client ID (registered at developers.globus.org)
MDF_AGENT_CLIENT_ID = "074cebcc-19ad-4332-bbf2-78402291b659"
REDIRECT_URI = "https://auth.globus.org/v2/web/auth-code"
APP_NAME = "MDF_Agent"

# Default token storage location
DEFAULT_TOKEN_PATH = Path("~/.config/mdf_agent/tokens.json").expanduser()

# =============================================================================
# MDF Connect Scopes
# =============================================================================

# Production MDF Connect
MDF_CONNECT_SCOPE = "https://auth.globus.org/scopes/4d5f8e8b-a61d-40d8-bb58-5a3f5d1d200a/connect"
MDF_CONNECT_RESOURCE_SERVER = "4d5f8e8b-a61d-40d8-bb58-5a3f5d1d200a"

# Development MDF Connect
MDF_CONNECT_DEV_SCOPE = "https://auth.globus.org/scopes/0e0a9538-ce45-43c1-998a-d3a7031a83f0/connect"
MDF_CONNECT_DEV_RESOURCE_SERVER = "0e0a9538-ce45-43c1-998a-d3a7031a83f0"

# =============================================================================
# Related Service Scopes (for clone, transfer, search)
# =============================================================================

TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"
SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:search"
SEARCH_INGEST_SCOPE = "urn:globus:auth:scope:search.api.globus.org:all"
# NCSA MDF collection — the HTTPS endpoint requires a scope tied to the
# collection UUID, not the hostname.
NCSA_MDF_COLLECTION_UUID = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"
DATA_MDF_SCOPE = f"https://auth.globus.org/scopes/{NCSA_MDF_COLLECTION_UUID}/https"
GROUPS_SCOPE = "urn:globus:auth:scope:groups.api.globus.org:view_my_groups_and_memberships"

# Scope sets for different operations
PUBLISH_SCOPES = [MDF_CONNECT_SCOPE]
PUBLISH_DEV_SCOPES = [MDF_CONNECT_DEV_SCOPE]
CLONE_SCOPES = [DATA_MDF_SCOPE, TRANSFER_SCOPE]
TRANSFER_SCOPES = [TRANSFER_SCOPE, DATA_MDF_SCOPE]


def get_scopes_for_service(service_instance: str = "prod") -> tuple[str, str]:
    """Get the appropriate scope and resource server for a service instance.

    Args:
        service_instance: Either "prod" or "dev"

    Returns:
        Tuple of (scope, resource_server_id)
    """
    if service_instance in ("dev", "development"):
        return MDF_CONNECT_DEV_SCOPE, MDF_CONNECT_DEV_RESOURCE_SERVER
    # staging uses prod auth (same Globus introspection)
    return MDF_CONNECT_SCOPE, MDF_CONNECT_RESOURCE_SERVER


def get_authorizer(
    token: Optional[str] = None,
    client_id: Optional[str] = None,
    scope: Optional[str] = None,
    service_instance: str = "prod",
    app_name: str = APP_NAME,
    token_path: Path = DEFAULT_TOKEN_PATH,
) -> AccessTokenAuthorizer | RefreshTokenAuthorizer:
    """Get a Globus authorizer for MDF Connect.

    This function supports three authentication methods:
    1. Direct token (passed as argument or via MDF_CONNECT_TOKEN env var)
    2. Interactive OAuth2 login (opens browser, user authenticates)
    3. Cached tokens from previous login (stored in ~/.config/mdf_agent/)

    Args:
        token: Optional access token. If not provided, checks MDF_CONNECT_TOKEN env var.
        client_id: Optional Globus client ID. Defaults to MDF_AGENT_CLIENT_ID.
        scope: Optional scope. If not provided, uses appropriate MDF Connect scope.
        service_instance: "prod" or "dev" - determines which MDF Connect instance to use.
        app_name: Application name for OAuth2 flow.
        token_path: Path to store cached tokens.

    Returns:
        A Globus authorizer ready for API calls.

    Raises:
        RuntimeError: If authentication fails or no valid auth method available.

    Examples:
        # Using environment variable
        >>> os.environ["MDF_CONNECT_TOKEN"] = "my-token"
        >>> auth = get_authorizer()

        # Using interactive login
        >>> auth = get_authorizer(service_instance="dev")

        # Using specific token
        >>> auth = get_authorizer(token="my-access-token")
    """
    # Check for direct token (argument or environment variable)
    token = token or os.getenv("MDF_CONNECT_TOKEN")
    if token:
        return AccessTokenAuthorizer(token)

    # Use defaults if not provided
    client_id = client_id or MDF_AGENT_CLIENT_ID

    # Get appropriate scope for service instance
    if scope is None:
        scope, resource_server = get_scopes_for_service(service_instance)
    else:
        resource_server = scope.split("/scopes/")[1].split("/")[0] if "/scopes/" in scope else scope

    # Ensure token directory exists
    token_path.parent.mkdir(parents=True, exist_ok=True)

    # Set up token storage and app config
    storage = JSONTokenStorage(str(token_path))
    config = GlobusAppConfig(
        token_storage=storage,
        request_refresh_tokens=True,
    )

    # Create UserApp with scope requirements
    app = UserApp(
        app_name,
        client_id=client_id,
        config=config,
    )

    # Add the required scope
    app.add_scope_requirements({resource_server: [scope]})

    # Login if needed (opens browser for user authentication)
    if app.login_required():
        print(f"Opening browser for Globus authentication...")
        print(f"If browser doesn't open, visit: {REDIRECT_URI}")
        app.login()

    return app.get_authorizer(resource_server)


def get_authorizer_for_scopes(
    scopes: list[str],
    client_id: Optional[str] = None,
    app_name: str = APP_NAME,
    token_path: Path = DEFAULT_TOKEN_PATH,
) -> dict[str, AccessTokenAuthorizer | RefreshTokenAuthorizer]:
    """Get authorizers for multiple scopes (e.g., for clone + transfer).

    Args:
        scopes: List of scope URIs to request.
        client_id: Optional Globus client ID.
        app_name: Application name for OAuth2 flow.
        token_path: Path to store cached tokens.

    Returns:
        Dictionary mapping resource server IDs to authorizers.
    """
    client_id = client_id or MDF_AGENT_CLIENT_ID

    token_path.parent.mkdir(parents=True, exist_ok=True)
    storage = JSONTokenStorage(str(token_path))
    config = GlobusAppConfig(
        token_storage=storage,
        request_refresh_tokens=True,
    )

    app = UserApp(app_name, client_id=client_id, config=config)

    # Build scope requirements
    scope_reqs = {}
    for scope in scopes:
        if "/scopes/" in scope:
            resource_server = scope.split("/scopes/")[1].split("/")[0]
        elif ":" in scope:
            # URN format: urn:globus:auth:scope:server:scope_name
            parts = scope.split(":")
            resource_server = parts[4] if len(parts) > 4 else scope
        else:
            resource_server = scope

        if resource_server not in scope_reqs:
            scope_reqs[resource_server] = []
        scope_reqs[resource_server].append(scope)

    app.add_scope_requirements(scope_reqs)

    if app.login_required():
        print("Opening browser for Globus authentication...")
        app.login()

    # UserApp always adds openid/profile/email for auth.globus.org;
    # include that authorizer so callers can use it for identity.
    all_servers = list(scope_reqs.keys())
    if "auth.globus.org" not in all_servers:
        all_servers.append("auth.globus.org")

    result = {}
    for rs in all_servers:
        try:
            result[rs] = app.get_authorizer(rs)
        except Exception:
            pass
    return result


def logout(token_path: Path = DEFAULT_TOKEN_PATH) -> bool:
    """Clear cached tokens and log out.

    Args:
        token_path: Path to token storage file.

    Returns:
        True if tokens were cleared, False if no tokens existed.
    """
    if token_path.exists():
        token_path.unlink()
        print("Logged out and cleared cached tokens.")
        return True
    return False


def is_logged_in(
    service_instance: str = "prod",
    client_id: Optional[str] = None,
    token_path: Path = DEFAULT_TOKEN_PATH,
) -> bool:
    """Check if user has valid cached tokens.

    Args:
        service_instance: "prod" or "dev"
        client_id: Optional Globus client ID.
        token_path: Path to token storage file.

    Returns:
        True if valid cached tokens exist.
    """
    if not token_path.exists():
        return False

    client_id = client_id or MDF_AGENT_CLIENT_ID
    scope, resource_server = get_scopes_for_service(service_instance)

    storage = JSONTokenStorage(str(token_path))
    config = GlobusAppConfig(token_storage=storage, request_refresh_tokens=True)
    app = UserApp(APP_NAME, client_id=client_id, config=config)
    app.add_scope_requirements({resource_server: [scope]})

    return not app.login_required()
