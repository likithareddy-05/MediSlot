#!/usr/bin/env bash
# MediSlot operations (Milestone 7). Run by .github/workflows/operations.yml and by the deploy
# pipeline (drift-check). Needs AWS credentials, AWS_REGION and ENVIRONMENT_NAME.
#
#   scripts/ops.sh stack-errors STACK     print why a stack deployment failed (used by the pipeline)
#   scripts/ops.sh clear-failed-create STACK  remove a stack left in ROLLBACK_COMPLETE (used by the pipeline)
#   scripts/ops.sh drift-check            detect drift on every stack; prints IN_SYNC / DRIFTED
#   scripts/ops.sh dr-drill               restore RDS (point in time) and the appointments table
#                                         (DynamoDB PITR) into temporary copies, measure the restore
#                                         times, verify the copies, delete them
#   scripts/ops.sh teardown CONFIRMATION  delete every stack (newest first), emptying the buckets;
#                                         CONFIRMATION must be DELETE-medislot-<env>
#
# Results are appended to $GITHUB_STEP_SUMMARY when it is set (the run summary in GitHub).
set -euo pipefail

: "${AWS_REGION:?AWS_REGION is required}"
: "${ENVIRONMENT_NAME:?ENVIRONMENT_NAME is required}"
ENV="$ENVIRONMENT_NAME"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

# Deployment order. Teardown walks it backwards.
STACKS=(network data security schema-init authentication workflows observability dashboard)

stack_name() { echo "medislot-${ENV}-$1"; }

stack_exists() {
  aws cloudformation describe-stacks --stack-name "$(stack_name "$1")" --region "$AWS_REGION" >/dev/null 2>&1
}

stack_output() {
  aws cloudformation describe-stacks --stack-name "$(stack_name "$1")" --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue | [0]" --output text
}

drift_check() {
  local drifted=0
  echo "### Drift detection (${ENV})" >>"$SUMMARY"
  echo "| Stack | Drift status |" >>"$SUMMARY"
  echo "|---|---|" >>"$SUMMARY"
  for short in "${STACKS[@]}"; do
    stack_exists "$short" || { echo "| $(stack_name "$short") | not deployed |" >>"$SUMMARY"; continue; }
    local id status detection
    id="$(aws cloudformation detect-stack-drift --stack-name "$(stack_name "$short")" --region "$AWS_REGION" \
      --query StackDriftDetectionId --output text)"
    for _ in $(seq 1 60); do
      detection="$(aws cloudformation describe-stack-drift-detection-status --stack-drift-detection-id "$id" \
        --region "$AWS_REGION" --query DetectionStatus --output text)"
      [[ "$detection" != "DETECTION_IN_PROGRESS" ]] && break
      sleep 5
    done
    status="$(aws cloudformation describe-stack-drift-detection-status --stack-drift-detection-id "$id" \
      --region "$AWS_REGION" --query StackDriftStatus --output text)"
    echo "$(stack_name "$short"): ${status} (${detection})"
    echo "| $(stack_name "$short") | ${status} |" >>"$SUMMARY"
    if [[ "$status" == "DRIFTED" ]]; then
      drifted=1
      aws cloudformation describe-stack-resource-drifts --stack-name "$(stack_name "$short")" --region "$AWS_REGION" \
        --stack-resource-drift-status-filters MODIFIED DELETED \
        --query "StackResourceDrifts[].[LogicalResourceId,StackResourceDriftStatus]" --output table || true
    fi
  done
  if [[ "$drifted" == 1 ]]; then
    echo "::warning::At least one stack has drifted from its template. Redeploy, or revert the manual change shown above."
  fi
}

dr_drill() {
  local source_db="medislot-${ENV}-mysql" drill_db="medislot-${ENV}-dr-drill"
  local table="medislot-${ENV}-appointments" drill_table="medislot-${ENV}-appointments-dr-drill"
  local subnet_group sg started finished rds_seconds ddb_seconds source_count restored_count

  echo "== RDS point-in-time restore of ${source_db} =="
  subnet_group="$(aws rds describe-db-instances --db-instance-identifier "$source_db" --region "$AWS_REGION" \
    --query "DBInstances[0].DBSubnetGroup.DBSubnetGroupName" --output text)"
  sg="$(aws rds describe-db-instances --db-instance-identifier "$source_db" --region "$AWS_REGION" \
    --query "DBInstances[0].VpcSecurityGroups[0].VpcSecurityGroupId" --output text)"
  started="$(date +%s)"
  aws rds restore-db-instance-to-point-in-time --region "$AWS_REGION" \
    --source-db-instance-identifier "$source_db" --target-db-instance-identifier "$drill_db" \
    --use-latest-restorable-time --db-instance-class db.t3.micro --db-subnet-group-name "$subnet_group" \
    --vpc-security-group-ids "$sg" --no-publicly-accessible --no-multi-az \
    --tags Key=Project,Value=MediSlot Key=Environment,Value="$ENV" Key=Purpose,Value=dr-drill >/dev/null
  aws rds wait db-instance-available --db-instance-identifier "$drill_db" --region "$AWS_REGION"
  finished="$(date +%s)"
  rds_seconds=$((finished - started))
  echo "RDS restore available after ${rds_seconds}s"
  aws rds describe-db-instances --db-instance-identifier "$drill_db" --region "$AWS_REGION" \
    --query "DBInstances[0].[DBInstanceStatus,Endpoint.Address,LatestRestorableTime]" --output table
  aws rds delete-db-instance --db-instance-identifier "$drill_db" --skip-final-snapshot \
    --delete-automated-backups --region "$AWS_REGION" >/dev/null

  echo "== DynamoDB point-in-time restore of ${table} =="
  started="$(date +%s)"
  aws dynamodb restore-table-to-point-in-time --region "$AWS_REGION" --source-table-name "$table" \
    --target-table-name "$drill_table" --use-latest-restorable-time >/dev/null
  aws dynamodb wait table-exists --table-name "$drill_table" --region "$AWS_REGION"
  until [[ "$(aws dynamodb describe-table --table-name "$drill_table" --region "$AWS_REGION" \
      --query Table.TableStatus --output text)" == "ACTIVE" ]]; do sleep 10; done
  finished="$(date +%s)"
  ddb_seconds=$((finished - started))
  source_count="$(aws dynamodb scan --table-name "$table" --select COUNT --region "$AWS_REGION" --query Count --output text)"
  restored_count="$(aws dynamodb scan --table-name "$drill_table" --select COUNT --region "$AWS_REGION" --query Count --output text)"
  echo "DynamoDB restore ACTIVE after ${ddb_seconds}s; items: source=${source_count} restored=${restored_count}"
  aws dynamodb delete-table --table-name "$drill_table" --region "$AWS_REGION" >/dev/null

  {
    echo "### DR drill ($(date -u +%Y-%m-%dT%H:%MZ), ${ENV})"
    echo "| Component | Restore method | Measured restore time | Verification |"
    echo "|---|---|---|---|"
    echo "| RDS MySQL \`${source_db}\` | point-in-time (latest restorable time) | ${rds_seconds} s | instance available, endpoint resolved |"
    echo "| DynamoDB \`${table}\` | PITR (latest restorable time) | ${ddb_seconds} s | items source=${source_count}, restored=${restored_count} |"
    echo
    echo "Temporary copies were deleted. Copy this table into docs/operations.md (DR drill record)."
  } >>"$SUMMARY"
}

empty_bucket() {
  local bucket="$1"
  [[ -n "$bucket" && "$bucket" != "None" ]] || return 0
  aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" >/dev/null 2>&1 || return 0
  echo "Emptying s3://${bucket} (all object versions)"
  while true; do
    local objects
    objects="$(aws s3api list-object-versions --bucket "$bucket" --region "$AWS_REGION" --max-items 500 --output json \
      | jq -c '[(.Versions // [])[], (.DeleteMarkers // [])[] | {Key, VersionId}]')"
    [[ "$(echo "$objects" | jq 'length')" -gt 0 ]] || break
    aws s3api delete-objects --bucket "$bucket" --region "$AWS_REGION" \
      --delete "$(echo "$objects" | jq -c '{Objects: ., Quiet: true}')" >/dev/null
  done
}

teardown() {
  local confirmation="${1:-}"
  [[ "$confirmation" == "DELETE-medislot-${ENV}" ]] || {
    echo "::error::Teardown needs the confirmation DELETE-medislot-${ENV}"; exit 1; }
  echo "### Teardown (${ENV})" >>"$SUMMARY"
  for (( i=${#STACKS[@]}-1; i>=0; i-- )); do
    local short="${STACKS[$i]}"
    stack_exists "$short" || continue
    if [[ "$short" == "data" ]]; then
      empty_bucket "$(stack_output data AssetsBucketName)"
      empty_bucket "$(stack_output data ReportsBucketName)"
    fi
    echo "Deleting $(stack_name "$short")"
    aws cloudformation delete-stack --stack-name "$(stack_name "$short")" --region "$AWS_REGION"
    aws cloudformation wait stack-delete-complete --stack-name "$(stack_name "$short")" --region "$AWS_REGION"
    echo "- deleted $(stack_name "$short")" >>"$SUMMARY"
  done
  echo "Teardown complete. The RDS instance left a final snapshot (DeletionPolicy: Snapshot)." | tee -a "$SUMMARY"
}

clear_failed_create() {
  # A stack whose FIRST creation failed sits in ROLLBACK_COMPLETE and can never be updated.
  # It holds no resources, so delete it and let the deploy create it again. Stacks that
  # failed an UPDATE (UPDATE_ROLLBACK_COMPLETE) are healthy and are left alone.
  local stack="$1" status
  status="$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
    --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "NONE")"
  if [[ "$status" == "ROLLBACK_COMPLETE" ]]; then
    echo "Stack ${stack} is ROLLBACK_COMPLETE from an earlier failed create; deleting it before deploying again."
    aws cloudformation delete-stack --stack-name "$stack" --region "$AWS_REGION"
    aws cloudformation wait stack-delete-complete --stack-name "$stack" --region "$AWS_REGION"
  fi
}

stack_errors() {
  # Print why a stack deployment failed (the pipeline calls this after a failed deploy).
  local stack="$1" status
  status="$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
    --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "UNKNOWN")"
  echo "::error::Stack ${stack} failed (status ${status}). First failure reasons, newest first:"
  aws cloudformation describe-stack-events --stack-name "$stack" --region "$AWS_REGION" --max-items 60 \
    --query "StackEvents[?contains(ResourceStatus,'FAILED') && ResourceStatusReason!='Resource update cancelled' && ResourceStatusReason!='Resource creation cancelled'].[Timestamp,LogicalResourceId,ResourceStatusReason]" \
    --output table || true
  if aws cloudformation describe-stack-events --stack-name "$stack" --region "$AWS_REGION" --max-items 60 \
      --query "StackEvents[].ResourceStatusReason" --output text 2>/dev/null \
      | grep -qiE "not authorized|AccessDenied|is not authorized to perform"; then
    echo "::error::The GitHub deploy role (secret AWS_ROLE_ARN) is missing a permission named above. Add it to that role and run the deploy again."
  fi
  case "$status" in
    *ROLLBACK_FAILED)
      echo "::error::The stack is stuck in ${status}. In CloudFormation choose 'Continue update rollback' (or delete the stack if it was never created), then rerun." ;;
    ROLLBACK_COMPLETE)
      echo "::error::The stack was never created successfully. Fix the reason above and rerun; the next deploy removes the failed stack automatically." ;;
  esac
}

case "${1:-}" in
  stack-errors) stack_errors "${2:?stack name required}" ;;
  clear-failed-create) clear_failed_create "${2:?stack name required}" ;;
  drift-check) drift_check ;;
  dr-drill) dr_drill ;;
  teardown) teardown "${2:-}" ;;
  *) echo "usage: $0 stack-errors STACK | clear-failed-create STACK | drift-check | dr-drill | teardown DELETE-medislot-<env>"; exit 2 ;;
esac
