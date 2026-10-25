
# MediSlot

MediSlot is an appointment booking platform designed to connect patients and doctors through a secure, reliable, and scalable system.

## Architecture

The planned architecture includes:

- Flask-based web application hosted on Amazon EC2
- Amazon API Gateway REST API for backend access
- AWS Lambda for authentication and business logic
- Amazon RDS for relational application data
- Amazon DynamoDB for slot coordination and workflow state
- Amazon EventBridge and Amazon SQS for event-driven processing
- AWS Step Functions for rescheduling workflows
- Amazon SNS for notifications
- AWS CloudFormation and GitHub Actions for infrastructure deployment and CI/CD

## Repository Structure

- `.github/workflows/` - CI and deployment workflows
- `infrastructure/` - CloudFormation templates
- `lambdas/` - AWS Lambda function code
- `docs/` - Architecture and project documentation

## Development Workflow

1. Develop changes on a feature branch.
2. Commit and push changes to the feature branch.
3. Run the required automated checks.
4. Open a pull request to `master` for review.
5. Merge only after the required review and checks pass.

## Security

- Do not commit credentials, tokens, passwords, or secret keys.
- Use AWS IAM roles and GitHub OIDC for deployment authentication.
- Store application secrets in AWS Systems Manager Parameter Store or an appropriate secrets service.
