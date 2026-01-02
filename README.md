# Credit Card Transaction Pipeline

> **Note**: Built in ~2+ hours as a data engineering exercise

A data pipeline implementing the Medallion Architecture (Bronze → Silver → Gold) for credit card transaction analysis and fraud detection.

## Overview

Processes 1.3M credit card transactions through three layers:
- **Bronze**: Raw CSV ingestion to Parquet
- **Silver**: Data cleaning and validation
- **Gold**: Customer and merchant aggregations
- **Insights**: Fraud analysis reporting

## Setup

**1. Download Dataset**

Get the dataset from Kaggle and place it in `data/raw/`:
- Link: https://www.kaggle.com/datasets/priyamchoksi/credit-card-transactions-dataset/data
- File: `data/raw/credit_card_transactions.csv`

**2. Install Dependencies**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**3. Run Pipeline**

```bash
python3 run_pipeline.py
```

## Testing

```bash
pytest tests/unit/ -v --cov=src
```

## Architecture

```
Raw CSV → Bronze (Parquet) → Silver (Validated) → Gold (Aggregations) → Insights
```

**Key Features:**
- Data quality validation (negative amounts, missing fields, future dates)
- Customer spending analysis
- Fraud rate analysis by merchant category
- Automated insights generation

## Output

The pipeline generates:
- `data/gold/customer_summary.parquet` - Customer metrics
- `data/gold/merchant_category_analysis.parquet` - Category analytics
- `data/gold/insights_report.txt` - Fraud analysis report


## CI/CD

GitHub Actions runs tests automatically on push.


## AI Tool Usage

I used GitHub Copilot during development to accelerate the implementation within the 2-hour timeframe. My approach was:

- **Architecture & Design**: I made all decisions regarding the Medallion architecture structure, layer responsibilities, and data flow patterns
- **Core Implementation**: I wrote the pipeline logic with Copilot's autocomplete suggestions for syntax
- **Pipeline Orchestration**: I used AI to help structure the main orchestrator (`run_pipeline.py`) and format the console output for better readability
- **Unit Tests**: Given the time constraint, I leveraged Copilot to generate the test cases for the silver and gold layers, which I then reviewed and validated
- **Documentation**: I used AI to help draft this README faster, providing structure and content that I then reviewed and customized
- **Debugging & Refinement**: I handled all debugging, error resolution, and architectural decisions independently

The AI served as a productivity tool to handle repetitive code patterns, but all architectural choices, validation logic, and business rules were my own decisions.

## Future Improvements

Given more time, I would enhance the project with:

**Code Quality**
- 
- Comprehensive docstrings for all functions with detailed parameter and return type documentation
- Inline comments explaining complex business logic and validation rules
- Type hints throughout the codebase for better support and error detection
- Robust error handling with try/except blocks and proper exception logging
- Retry logic for transient failures
- Pre-commit hooks with linting tools to maintain code quality standards during team collaboration

**Data Quality**
- Schema validation at ingestion using tools like Pydantic
- Data profiling and anomaly detection to catch unexpected patterns early
- Referential integrity checks between tables
- Business rule validation (e.g., transaction amounts within expected ranges, valid merchant categories)

**CI/CD & Environments**
- Multi-environment setup (DEV/TEST/PROD) with environment-specific configurations
- Separate deployment pipelines for each environment with promotion workflows
- Environment-specific variable management (database connections, S3 buckets, etc.)
- Integration tests running in PROD/TEST environment before PROD deployment


**Infrastructure & Deployment**
- LocalStack integration to simulate a realistic AWS environment locally
- Infrastructure as Code (AWS cdk/Terraform/CloudFormation) for reproducible deployments
- AWS Lambda functions for each pipeline layer
- S3 buckets for data lake storage (raw/processed/curated zones)
- EventBridge/Step Functions for pipeline orchestration
- CloudWatch for logging and monitoring

This would demonstrate a production-grade, cloud-native data pipeline architecture while maintaining the same medallion pattern logic.
