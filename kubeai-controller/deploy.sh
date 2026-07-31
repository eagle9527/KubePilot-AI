#!/bin/bash
set -e

echo "=== KubePilot AI Controller 部署脚本 ==="

# 1. 构建二进制
echo "[1/6] 构建 Linux 二进制..."
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/manager cmd/main.go

# 2. 构建镜像
echo "[2/6] 构建 Docker 镜像..."
docker build -t kubepilot-ai/kubeai-controller:local .

# 3. 加载到 kind
echo "[3/6] 加载镜像到 kind..."
kind load docker-image kubepilot-ai/kubeai-controller:local  -n kind1

# 4. 部署到 Kubernetes
echo "[4/6] 部署到 Kubernetes..."
kubectl apply -f deploy/namespace.yaml
kubectl apply -f deploy/crd.yaml
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/secret.yaml
kubectl apply -f deploy/deployment.yaml

# 5. 验证
echo "[5/6] 验证部署..."
sleep 5
kubectl get pods -n kubeai-system

# 6. 查看日志
echo "[6/6] 查看 Controller 日志..."
sleep 2
kubectl logs -n kubeai-system -l app.kubernetes.io/name=kubepilot-ai-controller --tail=20

echo ""
echo "=== 部署完成 ==="
echo ""
echo "常用命令:"
echo "  查看 Pod: kubectl get pods -n kubeai-system"
echo "  查看日志: kubectl logs -n kubeai-system -l app.kubernetes.io/name=kubepilot-ai-controller"
echo "  测试 AIIncident: kubectl apply -f deploy/example-aiincident.yaml"
