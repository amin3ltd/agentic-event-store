# Agentic Event Store - The Ledger

An enterprise-grade event sourcing infrastructure for multi-agent AI systems, built for Apex Financial Services to process commercial loan applications.

## Overview

This project implements **The Ledger** - a production-quality event store that provides:
- Immutable, append-only audit trail for all AI agent decisions
- Full decision history traceability for regulatory compliance
- Cryptographic integrity verification
- Temporal queries for debugging and compliance examination

## Scenario

Apex Financial Services processes 40-80 commercial loan applications per week. Applicants upload GAAP financial statements. AI agents read them, reason about them, and record every decision as an immutable event.

### The Four AI Agents

1. **DocumentProcessingAgent** - Extracts financial data from PDFs and Excel files
2. **CreditAnalysisAgent** - Evaluates financial risk and creditworthiness
3. **FraudDetectionAgent** - Screens for anomalous patterns in documents
4. **ComplianceAgent** - Verifies regulatory eligibility
5. **DecisionOrchestratorAgent** - Synthesizes all agent outputs for final recommendation

## Architecture

### Core Technologies
- **PostgreSQL** - Event store backend (append-only tables)
- **Python** - Async implementation with psycopg3
- **LangGraph** - Agent StateGraph orchestration
- **Anthropic Claude** - LLM for reasoning tasks
- **MCP** - Model Context Protocol for tool exposure

### Key Patterns

- **Event Sourcing** - Events as the source of truth, not just current state
- **CQRS** - Separate command and query models
- **Optimistic Concurrency Control** - Version-based conflict detection
- **Gas Town Pattern** - Agent session persistence via event replay

## Project Structure

```
agentic-event-store/
├── ledger/                 # Core event store implementation
│   ├── __init__.py
│   ├── event_store.py     # EventStore class
│   ├── schema.py          # PostgreSQL schema definitions
│   ├── events.py          # Event type definitions
│   └── aggregates/        # Domain aggregates
├── agents/                # LangGraph agent implementations
│   ├── base_agent.py      # BaseApexAgent base class
│   └── ...
├── projections/           # CQRS read models
│   └── ...
├── tests/                 # Test suite
├── docs/                  # Design documentation
└── README.md
```

## Getting Started

### Prerequisites
- Python 3.11+
- PostgreSQL 14+
- Anthropic API key

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Set up database
psql -c "CREATE DATABASE apex_ledger;"

# Run data generator
python datagen/generate_all.py --applicants 80 --db-url postgresql://localhost/apex_ledger
```

## Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Event Store Core - PostgreSQL Schema & Interface | 🔄 |
| 2 | Domain Logic - Aggregates, Commands & Business Rules | ⏳ |
| 3 | Projections - CQRS Read Models & Async Daemon | ⏳ |
| 4 | Upcasting, Integrity & The Gas Town Memory Pattern | ⏳ |

## The Week Standard

By end of this week, you must be able to demonstrate:
> "Show me the complete decision history of application ID X" — from first event to final decision, with every AI agent action, every compliance check, every human review, all causal links intact, temporal query to any point in the lifecycle, and cryptographic integrity verification.

## License

Training Project - 10 Academy TRP1
