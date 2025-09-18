# kubectl why-pending

A kubectl plugin that diagnoses why pods are stuck in Pending state by analyzing node constraints, taints, tolerations, and events.

## Features

- **Node Selector Analysis**: Checks if any nodes match the pod's nodeSelector requirements
- **Taint/Toleration Validation**: Identifies missing tolerations for node taints
- **Pod Events**: Shows recent events related to the pending pod
- **Karpenter Integration**: Optional diagnostics for Karpenter-managed clusters
- **NodePool Analysis**: Analyzes Karpenter NodePools for capacity and compatibility issues

## Installation

### Using krew (recommended)

```bash
kubectl krew install why-pending
```

### Manual Installation

```bash
# Download the binary for your platform
curl -L https://github.com/cilasbeltrame/why-pending/releases/latest/download/why-pending-$(uname -s)-$(uname -m) -o why-pending
chmod +x why-pending
sudo mv why-pending /usr/local/bin/kubectl-why-pending
```

## Usage

### Basic Usage

```bash
kubectl why-pending -n <namespace> -pod <pod-name>
```

### With Karpenter Diagnostics

```bash
kubectl why-pending -n <namespace> -pod <pod-name> --karpenter
```

### Advanced Options

```bash
kubectl why-pending \
  -n my-namespace \
  -pod my-pod \
  --kubeconfig ~/.kube/config \
  --context my-cluster \
  --karpenter \
  --karpenter-namespace karpenter \
  --karpenter-since 1h \
  --karpenter-raw-logs
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `-n, --namespace` | Target namespace | `default` |
| `-pod` | Pod name (required) | - |
| `--kubeconfig` | Path to kubeconfig file | `~/.kube/config` |
| `--context` | Kubernetes context | Current context |
| `--karpenter` | Enable Karpenter diagnostics | `false` |
| `--karpenter-namespace` | Karpenter controller namespace | `karpenter` |
| `--karpenter-since` | How far back to read logs | `30m` |
| `--karpenter-raw-logs` | Show raw log lines | `false` |

## Examples

### Diagnose a pending pod

```bash
kubectl why-pending -n production -pod web-app-7d4f8b9c6-xyz12
```

### Check with Karpenter diagnostics

```bash
kubectl why-pending -n production -pod web-app-7d4f8b9c6-xyz12 --karpenter
```

## Output

The plugin provides a structured analysis:

1. **Pod Status**: Current phase and scheduling conditions
2. **Node Selector**: How many nodes match the pod's requirements
3. **Taints/Tolerations**: Compatibility analysis with suggested fixes
4. **Pod Events**: Recent events that might explain the issue
5. **Karpenter Analysis** (optional): Controller logs and NodePool compatibility

## Common Issues Detected

- Missing nodeSelector matches
- Incompatible taints without proper tolerations
- Resource constraints in Karpenter NodePools
- Instance type or availability zone limitations
- Capacity limits reached

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

MIT License
