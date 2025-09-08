# How to Run the Educational AI Engine

## 1. Test DynamoDB Connection

First, make sure your AWS DynamoDB connection works:

\`\`\`bash
# Set up your environment variables in .env file first
python scripts/test_connection.py
\`\`\`

This will:
- Verify connection to your AWS DynamoDB tables
- List users found in investor_prod table
- Test data fetching for a sample user
- Show available content metadata

## 2. Process Single User by Email

Run the AI engine for one specific user:

\`\`\`bash
# Basic usage
python scripts/process_single_user.py user@example.com

# With verbose logging
python scripts/process_single_user.py user@example.com --verbose
\`\`\`

This will:
- Fetch all data for the specified user from your 8 DynamoDB tables
- Process and normalize the data into events
- Compute engagement features (recency, frequency, test scores)
- Run decision engine to determine email campaigns
- Send personalized emails if campaigns are triggered

## 3. Required Environment Variables

Make sure your `.env` file contains:

\`\`\`bash
# AWS DynamoDB Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1

# Email Service (optional for testing)
SENDGRID_API_KEY=your_sendgrid_key
FROM_EMAIL=noreply@yourdomain.com

# AI Service (optional)
OPENAI_API_KEY=your_openai_key
\`\`\`

## 4. Installation

\`\`\`bash
# Install dependencies
pip install -r requirements.txt

# Test the setup
python scripts/test_connection.py
