# **Pipeline Reliability Toolkit (Python)**

A production-minded, SRE-focused toolkit for building **resilient data pipelines**.  
Includes retry/backoff, schema validation, dead-letter queues, structured JSON logging, and a CLI for rapid triage.

This project showcases the reliability engineering mindset required for high-scale manufacturing environments like **Tesla’s Cell Software organization**, where system health, data quality, and rapid debugging are mission-critical.

---

##  **Overview**

Modern distributed pipelines fail in subtle ways:  
network flakiness, schema drift, malformed data, transient 5xx responses, hidden latency spikes, and partial processing.

**Pipeline Reliability Toolkit (PRT)** provides the foundational building blocks for making pipelines *resilient by design*:

-  **Exponential retry with jitter** for network operations  
-  **Schema validation** (Pydantic) to fail early & consistently  
-  **Dead-letter capture** with structured error taxonomy  
-  **Structured JSON logs** enriched with run_id, step, correlation_id  
-  **CLI tools** for validation, triage, and resilient HTTP calls  
-  **Consistent error codes** for clean observability and root-cause analysis  

Designed to mirror real-world SRE/support workflows in high-velocity production environments.

---

##  **Why I Built This**

This project demonstrates the exact skills Tesla looks for in **Software Support Engineers** and future **SRE/DevOps engineers**:

- Building tools that increase system reliability  
- Debugging distributed systems under pressure  
- Preventing failures through early validation  
- Creating structured logs that accelerate diagnosis  
- Handling malformed data safely (dead-lettering)  
- Designing predictable behaviors across failure modes  
- Thinking in terms of *observability, resilience, and system health*  

These are the same mental models required to support mission-critical manufacturing systems, where automation must be trustworthy, measurable, and debuggable.

---

##  **Core Features**

### **1. Retry & Backoff (Exponential + Jitter)**
- Handles network flakiness, 5xx responses, and transient failures  
- Preserves final exception  
- Annotates failures with consistent `error_code` taxonomy  

### **2. Schema Validation (Pydantic)**
- Fail fast on bad/missing fields  
- Enforces positive numbers, correct types, and time formats  
- Works on dicts, iterators, or Pandas DataFrames  

### **3. Dead-Letter Queue (JSONL)**
Captures invalid records as:
{"ts": "...", "error_code": "VALIDATION", "error_msg": "...", "record": {...}} 


### **4. Structured JSON Logging** 
-Every log includes: 
-ts (UTC ISO timestamp) 
-level 
-message 
-run_id 
-step 


### **5. Command-Line Interface**

validate   → Validate CSV/JSONL data & produce good/dead-letter files  
run-url    → Resilient HTTP GET with retries/backoff  
triage     → Summarize dead-letter failure patterns  

## **Usage Examples**
Validate a CSV file 
 
python3 pipeline_reliability_toolkit.py validate \  
    --in-file data.csv \  
    --schema builtins.ProductRecord \  
    --good-out out/good.jsonl \  
    --dead-letter out/dead_letter.jsonl  
Fetch a URL with retries 
 
python3 pipeline_reliability_toolkit.py run-url \  
    --url https://httpbin.org/status/500  
Summarize a dead-letter file  
 
python3 pipeline_reliability_toolkit.py triage \  
    --dead-letter out/dead_letter.jsonl  

## **Project Structure**   
  
pipeline_reliability_toolkit/  
│  
├── pipeline_reliability_toolkit.py     # Main toolkit + CLI  
├── README.md                           # This file  
│  
├── sample/  
│   ├── data.csv                        # Example pipeline input  
│   └── out/                            # (gitignored) output & DLQ  
   
Included Schema Example  
  
class ProductRecord(BaseModel):  
    id: str  
    name: str  
    price: float = Field(..., ge=0.0)  
    quantity: int = Field(..., ge=0)  
    ts: Optional[datetime] = None  
Invalid rows automatically route to the dead-letter file.  
  
## **Future Improvements**   
Add Marshmallow schema backend  
Add async/httpx retry pipeline  
Add automatic Prometheus-style counters for retries/failures  
Add URL pattern-based exception suppression  
Add a small web UI for dead-letter triage  
Add integration with Playwright for robust scraping pipelines  
  
## **About the Author — Richard Wilders**   
-Marine Corps veteran (Afghanistan — mission-critical language ops)  
-Background in data science, distributed pipelines, and observability  
-Strong interest in SRE, reliability engineering, and DevOps  
-Build tools that improve system health & debuggability  
-Completed ten 10-day Vipassana meditation courses — calm during incidents  
-Based in Reno/Sparks, NV  

This project is part of my engineering portfolio demonstrating reliability mindset and production-readiness for on-call, fast-paced environments like Tesla’s Cell Software organization.

## **Connect**. 
GitHub: https://github.com/richie-p-meyer    LinkedIn: https://www.linkedin.com/in/richard-wilders-915395106/.   
