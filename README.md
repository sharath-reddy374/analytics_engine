# EdYou AI Engine

A comprehensive educational AI engine that processes multi-source learning data to deliver personalized email campaigns and insights.

## Architecture Overview

The system follows a cloud-agnostic pipeline: **Ingest → Normalize → Understand → Feature → Decide → Email → Learn**

### Core Components

- **FastAPI Ingestor**: Bulk JSON ingestion from 8 educational data sources
- **PostgreSQL**: Operational database for users, events, and features  
- **MinIO**: S3-compatible data lake for raw JSON and Parquet files
- **ClickHouse**: Fast analytics engine for feature computation
- **Qdrant**: Vector database for conversation embeddings
- **RabbitMQ**: Message queue for async processing
- **LLM Workers**: Conversation understanding and email personalization
- **Airflow**: Orchestration for daily pipelines
- **SendGrid**: Email delivery with webhooks

## Quick Start

### 1. Start Services
\`\`\`bash
# Start all infrastructure services
make docker-up

# Wait 30 seconds for services to initialize, then seed sample data
make seed-data
\`\`\`

### 2. Run the Engine
\`\`\`bash
# Start the FastAPI application
make dev

# Or run with Docker
docker-compose up edyou-engine
\`\`\`

### 3. Test the API
\`\`\`bash
# Health check
curl http://localhost:8000/health

# View analytics dashboard
curl http://localhost:8000/analytics/dashboard

# Ingest sample data
curl -X POST http://localhost:8000/ingestor/bulk \
  -H "Content-Type: application/json" \
  -d '[{"source_table": "investor_prod", "data": {...}}]'
\`\`\`

### 4. Run Daily Pipeline
\`\`\`bash
# Manually trigger the daily pipeline
make run-pipeline
\`\`\`

## Data Sources Supported

The engine processes 8 educational data sources:

1. **investor_prod** - User profiles and conversation history
2. **conversation_history** - Chat interactions  
3. **InvestorLoginHistory_Prod** - Login events and device info
4. **User_Infinite_TestSeries_Prod** - Quiz attempts and scores
5. **TestSereiesRecord_Prod** - ACT Science test records
6. **LearningRecord_Prod** - Presentation viewing progress
7. **Question_Prod** - Generated educational content
8. **presentation_prod** - Course and presentation metadata
9. **ICP_Prod** - Individual course plans and lessons

## Key Features

### 🔄 Data Processing
- **Event Normalization**: Converts diverse data formats to unified event schema
- **Conversation Understanding**: LLM-powered analysis of learning conversations
- **Feature Engineering**: Daily computation of engagement, learning, and risk metrics
- **Real-time Processing**: Async event processing with RabbitMQ

### 🎯 Personalization Engine  
- **Rule-based Decisioning**: YAML-configurable email campaign rules
- **Smart Scheduling**: Respects user timezones and quiet hours
- **Content Matching**: Links users to relevant practice sets and videos
- **Churn Prevention**: Identifies at-risk learners for intervention

### 📧 Email Campaigns
- **Template System**: Jinja2 templates with LLM-generated personalization
- **Deliverability**: SPF/DKIM/DMARC compliance with unsubscribe handling
- **A/B Testing Ready**: Template versioning and performance tracking
- **Multi-provider**: SendGrid integration with fallback options

### 📊 Analytics & Monitoring
- **User Dashboards**: Engagement metrics and learning progress
- **Campaign Analytics**: Email performance and conversion tracking  
- **System Monitoring**: Health checks, error rates, and performance metrics
- **Data Quality**: Automated validation and anomaly detection

## Configuration

### Environment Variables
\`\`\`bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/edyou_engine
CLICKHOUSE_URL=clickhouse://localhost:9000/default
REDIS_URL=redis://localhost:6379/0

# Storage & Queue
MINIO_ENDPOINT=localhost:9000
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
QDRANT_URL=http://localhost:6333

# AI & Email
OPENAI_API_KEY=your-openai-key
SENDGRID_API_KEY=your-sendgrid-key
FROM_EMAIL=no-reply@edyou.com
\`\`\`

### Email Rules Configuration
Edit `config/email_rules.yaml` to customize campaign logic:

\`\`\`yaml
rules:
  - id: help_cells_bio
    when:
      all:
        - contains: { field: "top_topics", value: "Biology>Cells" }
        - lt: { field: "emails_sent_7d", value: 2 }
    action:
      template_id: "bio_cells_help_v1"
      priority: 90
      cooldown_days: 3
\`\`\`

## API Endpoints

### Core Ingestion
- `POST /ingestor/bulk` - Bulk data ingestion from source tables
- `POST /ingestor/single` - Single event ingestion

### Analytics  
- `GET /analytics/dashboard` - System overview and metrics
- `GET /users/{user_id}/features` - Individual user analytics

### Health & Monitoring
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics (if enabled)

## Development

### Setup Development Environment
\`\`\`bash
# Install dependencies
make install

# Start development server with hot reload
make dev

# Run tests
make test

# Code formatting
make format
make lint
\`\`\`

### Database Migrations
\`\`\`bash
# Generate migration
alembic revision --autogenerate -m "description"

# Apply migrations  
alembic upgrade head
\`\`\`

### Adding New Email Templates
1. Create HTML template in `templates/email/`
2. Add template config to `EmailService.templates`
3. Create corresponding rule in `config/email_rules.yaml`

## Deployment

### Production Deployment
\`\`\`bash
# Build and deploy with Docker
docker-compose -f docker-compose.prod.yml up -d

# Or deploy to Kubernetes
kubectl apply -f k8s/
\`\`\`

### Scaling Considerations
- **Horizontal Scaling**: Multiple FastAPI instances behind load balancer
- **Database Sharding**: Partition events by user_id for large datasets  
- **Queue Scaling**: Multiple RabbitMQ consumers for high throughput
- **Cache Layer**: Redis for frequently accessed user features

## Monitoring & Observability

### Metrics Tracked
- **Ingestion**: Events/second, processing latency, error rates
- **Features**: Computation time, data freshness, accuracy
- **Emails**: Delivery rates, open/click rates, unsubscribe rates
- **System**: CPU/memory usage, database connections, queue depth

### Alerting
- Email complaint rate > 0.1%
- Queue backlog > 1000 messages  
- Feature computation failures
- Database connection issues

## Security & Privacy

### Data Protection
- **PII Minimization**: Only store necessary user data
- **Encryption**: TLS in transit, encrypted storage at rest
- **Access Control**: Role-based permissions and API keys
- **Audit Logging**: All data access and modifications logged

### Compliance
- **FERPA/COPPA**: K-12 education compliance ready
- **GDPR**: Right to deletion and data portability
- **CAN-SPAM**: Compliant email practices with unsubscribe