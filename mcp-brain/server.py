import os
import requests
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from mcp.server.fastmcp import FastMCP

# 1. Initialize FastMCP and FastAPI
mcp = FastMCP("Kafka-Orchestrator-Brain")
app = FastAPI(title="MCP Failure Orchestrator Brain")

# DOCKER FIX: Use environment variable to find the Go API.
# Inside Docker Compose, this will resolve to http://ingestion-api:8080
GO_API_URL = os.getenv("GO_API_URL", "http://localhost:8080")

# --- CORE BUSINESS LOGIC ---

@mcp.tool()
def handle_failure_event(event_id: str):
    """
    Orchestration Logic with Deep Logging:
    Tracks the analysis from context fetching to final decision execution.
    """

    # FIX: Changed outer quotes to f''' or used single quotes inside to avoid SyntaxError
    print(f"\n{'='*50}")
    print(f"🔍 [START] Analyzing Event: {event_id}")
    print(f"{'='*50}")
    
    try:
        # Step 1: Fetching Context
        context_url = f"{GO_API_URL}/tools/failures/{event_id}"
        print(f"📡 [1/5] Requesting context from Go API at {context_url}...")
        
        resp = requests.get(context_url, timeout=5)
        if resp.status_code != 200:
            print(f"❌ [ERROR] Go API returned status {resp.status_code}. Event context missing.")
            return f"Event {event_id} not found."

        data = resp.json()
        event_data = data.get('event', {})
        retry_count = data.get('retry_count', 0)
        exc_type = event_data.get('exception_type', "Unknown")
        error_msg = event_data.get('error_message', "").lower()

        print(f"📊 [2/5] Metadata Extracted:")
        print(f"    - Exception: {exc_type}")
        print(f"    - Retry Count: {retry_count}")
        print(f"    - Error Message: {error_msg[:100]}...")

        # Step 2: Intelligent Decision Tree
        print(f"🧠 [3/5] Running Decision Engine...")
        decision = "DLQ"
        reason = ""

        # Case A: Safety Threshold
        if retry_count >= 3:
            decision = "DLQ"
            reason = f"MAX_RETRIES_EXCEEDED: Already attempted {retry_count} times. Quarantining."
            print(f"🚨 [RULE] Max retry limit hit.")

        # Case B: Permanent Logic Errors (Poison Pills)
        elif any(keyword in exc_type for keyword in ["NullPointer", "ValidationError", "Syntax", "Index"]):
            decision = "DLQ"
            reason = f"CRITICAL_LOGIC_ERROR: {exc_type} is a code bug. Retries will fail."
            print(f"🚫 [RULE] Poison pill detected: {exc_type}")

        # Case C: Transient Network Issues
        elif any(keyword in exc_type for keyword in ["Timeout", "Connection", "Network", "Broker"]):
            decision = "RETRY"
            reason = f"TRANSIENT_FAILURE: {exc_type} detected. Attempting recovery."
            print(f"♻️ [RULE] Network issue detected. Scheduling retry.")

        # Case D: Database Contention
        elif "deadlock" in error_msg or "database" in exc_type.lower():
            decision = "RETRY"
            reason = "RESOURCE_CONTENTION: Database deadlock/timeout. Retrying with backoff."
            print(f"🗄️ [RULE] Database contention found.")

        # Case E: Unknown
        else:
            decision = "PENDING"
            reason = f"UNKNOWN_EXCEPTION: No rule for {exc_type}. Flagging for human review."
            print(f"❓ [RULE] No matching rule. Escalating.")

        # Step 3: Executing Decision
        print(f"⚖️ [4/5] FINAL DECISION: {decision}")
        print(f"📝 [4/5] REASON: {reason}")

        decision_payload = {
            "event_id": event_id,
            "decision": decision,
            "reason": reason
        }
        
        print(f"📤 [5/5] Sending decision to Go Executor...")
        action_resp = requests.post(
            f"{GO_API_URL}/tools/decisions", 
            json=decision_payload, 
            timeout=5
        )

        if action_resp.status_code == 200:
            print(f"✅ [SUCCESS] Decision loop complete for {event_id}.")
            print(f"{'='*50}\n")
            return f"Processed {event_id} -> {decision}"
        else:
            print(f"⚠️ [WARNING] Go API failed to acknowledge decision: {action_resp.text}")
            return "Decision made, but Go API update failed."

    except Exception as e:
        print(f"💥 [CRITICAL ERROR] {str(e)}")
        return f"Brain Error: {str(e)}"

# --- API ENDPOINTS FOR GO EXECUTOR ---

@app.post("/tools/handle_failure_event")
async def trigger_tool(payload: dict, background_tasks: BackgroundTasks):
    """
    This endpoint is called by the Go Executor.
    """
    event_id = payload.get("event_id")
    if not event_id:
        return {"error": "No event_id provided"}, 400
    
    background_tasks.add_task(handle_failure_event, event_id)
    
    return {
        "status": "accepted",
        "message": f"Brain is now analyzing {event_id} in the background.",
        "event_id": event_id
    }

@app.get("/health")
def health_check():
    return {"status": "brain_active", "mcp_version": "1.0.0"}

# --- SERVER STARTUP ---

if __name__ == "__main__":
    print(f"🚀 Starting MCP Brain with GO_API_URL: {GO_API_URL}")
    uvicorn.run(app, host="0.0.0.0", port=8000)