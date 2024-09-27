from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from sqlalchemy.orm import Session

from danswer.configs.app_configs import AUTH_TYPE
from danswer.configs.constants import AuthType
from danswer.db.engine import get_session
from danswer.db.models import User
from danswer.utils.logger import setup_logger
from ee.danswer.auth.api_key import get_hashed_api_key_from_request
from ee.danswer.db.api_key import fetch_user_for_api_key
from ee.danswer.db.saml import get_saml_account
from ee.danswer.server.seeding import get_seed_config
from ee.danswer.utils.secrets import extract_hashed_cookie
import jwt
from danswer.configs.app_configs import DATA_PLANE_SECRET
from danswer.configs.app_configs import EXPECTED_API_KEY


logger = setup_logger()

def verify_auth_setting() -> None:
    # All the Auth flows are valid for EE version
    logger.notice(f"Using Auth Type: {AUTH_TYPE.value}")


async def control_plane_dep(request: Request) -> None:
    auth_header = request.headers.get("Authorization")
    api_key = request.headers.get("X-API-KEY")

    if api_key != EXPECTED_API_KEY:
        logger.warning("Invalid API key")
        raise HTTPException(status_code=401, detail="Invalid API key")

    if not auth_header or not auth_header.startswith("Bearer "):
        logger.warning("Invalid authorization header")
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, DATA_PLANE_SECRET, algorithms=["HS256"])
        if payload.get("scope") != "tenant:create":
            logger.warning("Insufficient permissions")
            raise HTTPException(status_code=403, detail="Insufficient permissions")
    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        logger.warning("Invalid token")
        raise HTTPException(status_code=401, detail="Invalid token")


async def optional_user_(
    request: Request,
    user: User | None,
    db_session: Session,
) -> User | None:
    # Check if the user has a session cookie from SAML
    if AUTH_TYPE == AuthType.SAML:
        saved_cookie = extract_hashed_cookie(request)

        if saved_cookie:
            saml_account = get_saml_account(cookie=saved_cookie, db_session=db_session)
            user = saml_account.user if saml_account else None

    # check if an API key is present
    if user is None:
        hashed_api_key = get_hashed_api_key_from_request(request)
        if hashed_api_key:
            user = fetch_user_for_api_key(hashed_api_key, db_session)

    return user



def api_key_dep(
    request: Request, db_session: Session = Depends(get_session)
) -> User | None:
    if AUTH_TYPE == AuthType.DISABLED:
        return None

    hashed_api_key = get_hashed_api_key_from_request(request)
    if not hashed_api_key:
        raise HTTPException(status_code=401, detail="Missing API key")

    if hashed_api_key:
        user = fetch_user_for_api_key(hashed_api_key, db_session)

    if user is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return user



def get_default_admin_user_emails_() -> list[str]:
    seed_config = get_seed_config()
    if seed_config and seed_config.admin_user_emails:
        return seed_config.admin_user_emails
    return []
