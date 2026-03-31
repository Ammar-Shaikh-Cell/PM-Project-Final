from fastapi import APIRouter

router = APIRouter(prefix="/knowledge-base", tags=["knowledge"])


@router.get("")
def list_articles():
    """Return knowledge base articles. No mock data - returns empty until real content is configured."""
    return []


@router.get("/chatops")
def chatops_stub():
    return {
        "status": "ready",
        "message": "ChatOps integration stub. Connect Slack or Teams bot here.",
        "supported_commands": ["/pm status", "/pm ack <alarm_id>", "/pm report daily"],
    }

