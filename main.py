import os
import requests
import logging
from datetime import datetime, timedelta
from time import sleep
from typing import List, Optional
from pydantic import BaseModel, ValidationError, Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# --- Configuration Model ---
class Settings(BaseSettings):
    zoom_account_id: str = Field(..., env="ZOOM_ACCOUNT_ID")
    zoom_client_id: str = Field(..., env="ZOOM_CLIENT_ID")
    zoom_client_secret: str = Field(..., env="ZOOM_CLIENT_SECRET")
    ghl_webhook_url: str = Field(..., env="GHL_WEBHOOK_URL")
    webinar_id: str = Field(..., env="WEBINAR_ID")
    page_size: int = Field(30, env="PAGE_SIZE")
    max_retries: int = Field(3, env="MAX_RETRIES")
    rate_limit_delay: float = Field(1.2, env="RATE_LIMIT_DELAY")

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

# --- Pydantic Models ---
class Participant(BaseModel):
    id: str
    name: str
    user_id: str
    registrant_id: str
    user_email: str
    join_time: datetime
    leave_time: datetime
    duration: int
    failover: bool
    status: str
    internal_user: bool

class WebinarParticipantsResponse(BaseModel):
    next_page_token: Optional[str] = None
    page_count: int
    page_size: int
    participants: List[Participant]
    total_records: int

# --- Zoom API Client ---
class ZoomAPIClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.base_url = "https://api.zoom.us/v2/past_webinars"
        self.token_expires_at = None
        self.access_token = None
        self.required_scopes = {
            "webinar:read:webinar:admin",
            "webinar:read:list_past_participants:admin"
        }

    def _get_access_token(self):
        """Obtain and validate OAuth access token"""
        try:
            auth_url = "https://zoom.us/oauth/token"
            auth_payload = {
                "grant_type": "account_credentials",
                "account_id": self.settings.zoom_account_id
            }

            response = requests.post(
                auth_url,
                auth=(self.settings.zoom_client_id, self.settings.zoom_client_secret),
                data=auth_payload,
                timeout=10
            )
            response.raise_for_status()

            token_data = response.json()
            self._validate_token_scopes(token_data['scope'])
            
            self.access_token = token_data['access_token']
            self.token_expires_at = datetime.now() + timedelta(seconds=token_data['expires_in'] - 60)
            logger.info("Successfully obtained Zoom access token")

        except requests.exceptions.RequestException as e:
            logger.error(f"Token request failed: {str(e)}")
            raise
        except KeyError as e:
            logger.error(f"Malformed token response: {str(e)}")
            raise

    def _validate_token_scopes(self, granted_scopes: str):
        """Verify required scopes are present"""
        granted = set(granted_scopes.split())
        missing = self.required_scopes - granted
        
        if missing:
            logger.error(f"Missing required scopes: {', '.join(missing)}")
            raise PermissionError(f"Missing scopes: {', '.join(missing)}")

    def _refresh_token_if_needed(self):
        """Check and refresh token if expired"""
        if not self.access_token or datetime.now() >= self.token_expires_at:
            logger.info("Refreshing Zoom access token")
            self._get_access_token()

    def get_participants(self, next_page_token: Optional[str] = None) -> WebinarParticipantsResponse:
        """Retrieve webinar participants with pagination"""
        self._refresh_token_if_needed()
        retries = 0

        while retries <= self.settings.max_retries:
            try:
                params = {
                    "page_size": self.settings.page_size,
                    "next_page_token": next_page_token
                }

                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                }

                url = f"{self.base_url}/{self.settings.webinar_id}/participants"
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=15
                )
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds")
                    sleep(retry_after)
                    continue

                response.raise_for_status()
                return WebinarParticipantsResponse(**response.json())

            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {str(e)}")
                retries += 1
                if retries <= self.settings.max_retries:
                    sleep(2 ** retries)
                else:
                    raise

# --- GHL Webhook Handler ---
def send_to_ghl(participant: Participant, webhook_url: str) -> bool:
    """Send individual participant data to GHL webhook"""
    try:
        payload = {
            "email": participant.user_email,
            "first_name": participant.name.split()[0],
            "last_name": " ".join(participant.name.split()[1:]) if " " in participant.name else "",
            "_update": True,
            "custom_fields": {
                "last_webinar_attended": "Amazon Masterclass",
                "last_webinar_attended_date": participant.join_time.isoformat(),
                "webinar_attendance_count": "INCREMENT"
            }
        }

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "ZoomWebinarIntegration/1.0"
        }

        response = requests.post(
            webhook_url,
            headers=headers,
            json=payload,
            timeout=10
        )

        if response.status_code == 404:
            logger.warning(f"Contact not found: {participant.user_email}")
            return False

        response.raise_for_status()
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Webhook failed for {participant.user_email}: {str(e)}")
        return False

# --- Main Execution Flow ---
def main():
    all_participants = []
    settings = Settings()
    zoom = ZoomAPIClient(settings)
    
    all_participants = []
    next_token = None

    try:
        logger.info(f"Starting processing for webinar {settings.webinar_id}")
        
        while True:
            response = zoom.get_participants(next_token)
            all_participants.extend(response.participants)
            logger.info(f"Retrieved page with {len(response.participants)} participants")

            if not response.next_page_token:
                break

            next_token = response.next_page_token
            sleep(settings.rate_limit_delay)

        logger.info(f"Total participants retrieved: {len(all_participants)}")
        for participant in all_participants:
            print(participant.dict(), end="\n\n") 

        # # Process participants with rate limiting
        # success_count = 0
        # for idx, participant in enumerate(all_participants, 1):
        #     if send_to_ghl(participant, settings.ghl_webhook_url):
        #         success_count += 1
            
        #     # Rate limiting between requests
        #     if idx % 10 == 0:
        #         sleep(settings.rate_limit_delay)

        # logger.info(f"Successfully processed {success_count}/{len(all_participants)} participants")

    except ValidationError as e:
        logger.error(f"Data validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        raise

if __name__ == "__main__":
    main()