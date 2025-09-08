# ECS Deployment Guide

This guide explains how to deploy the Rust Agent Sidecar to Amazon ECS.

## Prerequisites

1. **AWS CLI configured** with appropriate permissions
2. **ECS cluster** already created
3. **ECR repository** for storing the sidecar image
4. **EFS file system** for storing WASM modules
5. **IAM roles** with necessary permissions

## Step 1: Build and Push the Sidecar Image

### Build the Image

```bash
# Build the sidecar image
./build.sh

# Tag for ECR
docker tag rustagent-sidecar:latest YOUR_ACCOUNT.dkr.ecr.YOUR_REGION.amazonaws.com/rustagent-sidecar:latest
```

### Push to ECR

```bash
# Login to ECR
aws ecr get-login-password --region YOUR_REGION | docker login --username AWS --password-stdin YOUR_ACCOUNT.dkr.ecr.ecr.YOUR_REGION.amazonaws.com

# Push the image
docker push YOUR_ACCOUNT.dkr.ecr.YOUR_REGION.amazonaws.com/rustagent-sidecar:latest
```

## Step 2: Prepare WASM Modules

### Build the Example WASM Module

```bash
cd examples/rust-log-processor
./build-wasm.sh
cd ../..
```

### Upload to EFS

```bash
# Mount EFS locally (if needed)
sudo mkdir -p /mnt/efs
sudo mount -t efs YOUR_EFS_ID:/ /mnt/efs

# Copy WASM modules
sudo cp wasm/*.wasm /mnt/efs/
sudo umount /mnt/efs
```

## Step 3: Create IAM Roles

### Task Execution Role

Create a role with the following policies:
- `AmazonECSTaskExecutionRolePolicy`
- Custom policy for ECR access

### Task Role

Create a role with permissions for:
- CloudWatch Logs
- EFS access (if needed)
- Any other AWS services your WASM modules might need

## Step 4: Update Task Definition

1. Copy `ecs-task-definition.json`
2. Update the following values:
   - `executionRoleArn`: Your task execution role ARN
   - `taskRoleArn`: Your task role ARN
   - `image`: Your ECR image URI
   - `fileSystemId`: Your EFS file system ID
   - `awslogs-group`: Your CloudWatch log group
   - `awslogs-region`: Your AWS region

## Step 5: Deploy to ECS

### Create Log Group

```bash
aws logs create-log-group --log-group-name /ecs/app-with-sidecar --region YOUR_REGION
```

### Register Task Definition

```bash
aws ecs register-task-definition --cli-input-json file://ecs-task-definition.json --region YOUR_REGION
```

### Run the Task

```bash
aws ecs run-task \
  --cluster YOUR_CLUSTER_NAME \
  --task-definition app-with-sidecar \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-12345,subnet-67890],securityGroups=[sg-12345],assignPublicIp=ENABLED}" \
  --region YOUR_REGION
```

## Step 6: Monitor and Verify

### Check Task Status

```bash
aws ecs describe-tasks \
  --cluster YOUR_CLUSTER_NAME \
  --tasks TASK_ARN \
  --region YOUR_REGION
```

### View Logs

```bash
# View sidecar logs
aws logs tail /ecs/app-with-sidecar --follow --since 1h

# View application logs
aws logs tail /ecs/app-with-sidecar --follow --since 1h --log-stream-name app
```

## Step 7: Create a Service (Optional)

For production use, create an ECS service:

```bash
aws ecs create-service \
  --cluster YOUR_CLUSTER_NAME \
  --service-name app-with-sidecar-service \
  --task-definition app-with-sidecar \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-12345,subnet-67890],securityGroups=[sg-12345],assignPublicIp=ENABLED}" \
  --region YOUR_REGION
```

## Configuration Examples

### Environment Variables

```bash
# Set via ECS task definition
SIDECAR_TARGET_CONTAINER=myapp
SIDECAR_LOG_INTERVAL_MS=5000
SIDECAR_WASM_MODULE_PATH=/wasm/custom-processor.wasm
RUST_LOG=info
```

### Custom WASM Module Path

```bash
# Mount different WASM modules
SIDECAR_WASM_MODULE_PATH=/wasm/production-processor.wasm
```

## Troubleshooting

### Common Issues

1. **Container fails to start**
   - Check IAM roles and permissions
   - Verify ECR image exists and is accessible
   - Check security group rules

2. **WASM module not found**
   - Verify EFS volume is mounted correctly
   - Check file permissions
   - Ensure WASM file exists in the specified path

3. **Permission denied on Docker socket**
   - This is expected in ECS - the sidecar will use container logs instead
   - Update the code to handle ECS environment gracefully

4. **High memory usage**
   - Adjust `log_interval_ms` to reduce frequency
   - Implement log batching in the WASM module
   - Monitor CloudWatch metrics

### Debug Mode

Enable debug logging in production:

```bash
RUST_LOG=debug
```

## Security Best Practices

1. **Use least privilege IAM roles**
2. **Enable VPC endpoints** for AWS services
3. **Use EFS encryption** for WASM modules
4. **Regularly update** the sidecar image
5. **Monitor CloudTrail** for API access

## Cost Optimization

1. **Use Spot instances** for non-critical workloads
2. **Right-size** CPU and memory allocation
3. **Monitor EFS costs** for WASM storage
4. **Use CloudWatch Insights** for log analysis

## Next Steps

1. **Implement custom WASM modules** for your specific use case
2. **Add metrics collection** and alerting
3. **Set up CI/CD pipeline** for automated deployments
4. **Implement log retention policies**
5. **Add health checks** and auto-scaling 