package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	ns := flag.String("n", "default", "namespace")
	podName := flag.String("pod", "", "pod name (required)")
	kubeconfig := flag.String("kubeconfig", defaultKubeconfig(), "kubeconfig path")
	contextName := flag.String("context", "", "kube context (optional)")

	// Karpenter diagnostics flags
	enableKarpenter := flag.Bool("karpenter", false, "enable Karpenter diagnostics (fetch and scan controller logs)")
	karpenterNamespace := flag.String("karpenter-namespace", "karpenter", "namespace where Karpenter controller runs")
	karpenterSelector := flag.String("karpenter-selector", "app.kubernetes.io/name=karpenter", "label selector to find Karpenter controller pods")
	karpenterSince := flag.Duration("karpenter-since", 30*time.Minute, "how far back to read Karpenter logs (e.g. 10m, 1h)")
	karpenterMaxBytes := flag.Int("karpenter-max-bytes", 1_000_000, "maximum bytes to read from each Karpenter pod logs")
	karpenterRaw := flag.Bool("karpenter-raw-logs", false, "print raw Karpenter log lines (default: summary only)")
	karpenterOnlyMatching := flag.Bool("karpenter-only-matching", true, "show only NodePools/Provisioners relevant to the target pod")
	flag.Parse()

	if *podName == "" {
		fmt.Println("usage: why-pending -n <namespace> -pod <name> [--kubeconfig <path>] [--context <name>]")
		os.Exit(2)
	}

	ctx := context.Background()
	cfg, err := getRestConfig(*kubeconfig, *contextName)
	fatalIf(err)
	cli, err := kubernetes.NewForConfig(cfg)
	fatalIf(err)
	dyn, err := dynamic.NewForConfig(cfg)
	fatalIf(err)

	pod, err := cli.CoreV1().Pods(*ns).Get(ctx, *podName, metav1.GetOptions{})
	fatalIf(err)

	if pod.Status.Phase != corev1.PodPending {
		fmt.Printf("Pod %s/%s is %s (not Pending)\n", *ns, *podName, pod.Status.Phase)
		return
	}

	fmt.Printf("Pod: %s/%s  Phase: %s\n", *ns, *podName, pod.Status.Phase)
	printSchedCond(pod)

	// 1) list nodes
	nl, err := cli.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	fatalIf(err)
	nodes := nl.Items

	// 2) filter by NodeSelector (simple MVP; ignores Affinity for now)
	candidates := filterByNodeSelector(&pod.Spec, nodes)

	fmt.Printf("\n[1] NodeSelector: %d/%d nodes match\n", len(candidates), len(nodes))
	if len(candidates) == 0 {
		fmt.Println("Root cause: no nodes match pod.spec.nodeSelector")
		printSelectorHint(pod)
		// still allow optional karpenter diagnostics before exiting
		if *enableKarpenter {
			fmt.Println("\n[extra] Karpenter controller diagnostics:")
			hits, err := runKarpenterDiagnostics(ctx, cli, *karpenterNamespace, *karpenterSelector, *ns, *podName, *karpenterSince, *karpenterMaxBytes, *karpenterRaw)
			if err != nil {
				fmt.Println("  (karpenter diagnostics error:", err, ")")
			}
            if hits > 0 {
                runKarpenterDeepDive(ctx, dyn, *ns, *podName, *karpenterOnlyMatching)
			}
		}
		return
	}

	// 3) filter by Taints/Tolerations (NoSchedule/NoExecute)
	survivors, missing := filterByTaints(&pod.Spec, candidates)
	fmt.Printf("[2] Taints/Tolerations: %d/%d nodes pass\n", len(survivors), len(candidates))
	if len(survivors) == 0 {
		fmt.Println("Root cause: missing tolerations for node taints")
		printMissingTolerations(missing)
		printTolerationSnippets(missing)
		// still allow optional karpenter diagnostics before exiting
		if *enableKarpenter {
			fmt.Println("\n[extra] Karpenter controller diagnostics:")
			hits, err := runKarpenterDiagnostics(ctx, cli, *karpenterNamespace, *karpenterSelector, *ns, *podName, *karpenterSince, *karpenterMaxBytes, *karpenterRaw)
			if err != nil {
				fmt.Println("  (karpenter diagnostics error:", err, ")")
			}
            if hits > 0 {
                runKarpenterDeepDive(ctx, dyn, *ns, *podName, *karpenterOnlyMatching)
			}
		}
		return
	}

	// (optional next steps: topology spread, resources, volumes…)
	fmt.Println("\nNo hard blockers found in MVP gates (NodeSelector/Taints).")
	fmt.Println("Next: implement resources, topologySpread, volume(AZ) checks.")

	// Optional: Karpenter diagnostics
	if *enableKarpenter {
		fmt.Println("\n[extra] Karpenter controller diagnostics:")
		hits, err := runKarpenterDiagnostics(ctx, cli, *karpenterNamespace, *karpenterSelector, *ns, *podName, *karpenterSince, *karpenterMaxBytes, *karpenterRaw)
		if err != nil {
			fmt.Println("  (karpenter diagnostics error:", err, ")")
		}
        if hits > 0 {
            runKarpenterDeepDive(ctx, dyn, *ns, *podName, *karpenterOnlyMatching)
		}
	}
}

func getRestConfig(kubeconfigPath, contextName string) (*rest.Config, error) {
    var cfg *rest.Config
    var err error
    if inCluster() {
        cfg, err = rest.InClusterConfig()
    } else {
        loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath}
        over := &clientcmd.ConfigOverrides{}
        if contextName != "" {
            over.CurrentContext = contextName
        }
        rc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, over)
        cfg, err = rc.ClientConfig()
    }
    if err != nil {
        return nil, err
    }
    return cfg, nil
}

func inCluster() bool {
	_, s := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	return s
}

func defaultKubeconfig() string {
	if p, ok := os.LookupEnv("KUBECONFIG"); ok && p != "" {
		return p
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".kube", "config")
}

func printSchedCond(p *corev1.Pod) {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled {
			fmt.Printf("  PodScheduled=%s  Reason=%s  Message=%s  (age %s)\n",
				c.Status, c.Reason, trim(c.Message, 140), age(c.LastTransitionTime.Time))
		}
	}
}

func filterByNodeSelector(ps *corev1.PodSpec, nodes []corev1.Node) []corev1.Node {
	if len(ps.NodeSelector) == 0 {
		return nodes
	}
	sel := klabels.SelectorFromSet(ps.NodeSelector)
	var out []corev1.Node
	for _, n := range nodes {
		if sel.Matches(klabels.Set(n.Labels)) {
			out = append(out, n)
		}
	}
	return out
}

type missingTol struct {
	Key, Value, Effect string
	Nodes              []string
}

func filterByTaints(ps *corev1.PodSpec, nodes []corev1.Node) ([]corev1.Node, []missingTol) {
	var out []corev1.Node
	missing := make(map[string]*missingTol) // key: k=v:effect
	for _, n := range nodes {
		if t := firstUntoleratedTaint(n.Spec.Taints, ps.Tolerations); t != nil {
			k := fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect)
			entry := missing[k]
			if entry == nil {
				entry = &missingTol{Key: t.Key, Value: t.Value, Effect: string(t.Effect)}
				missing[k] = entry
			}
			entry.Nodes = append(entry.Nodes, n.Name)
			continue
		}
		out = append(out, n)
	}
	var list []missingTol
	for _, v := range missing {
		sort.Strings(v.Nodes)
		list = append(list, *v)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })
	return out, list
}

// MVP: treat NoSchedule/NoExecute taints as blockers unless tolerated via:
//   - Equal (key/value/effect match), or
//   - Exists (key matches; effect matches if set).
func firstUntoleratedTaint(taints []corev1.Taint, tolerations []corev1.Toleration) *corev1.Taint {
	for i := range taints {
		t := taints[i]
		if t.Effect != corev1.TaintEffectNoSchedule && t.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if tolerates(t, tolerations) {
			continue
		}
		return &t
	}
	return nil
}

func tolerates(taint corev1.Taint, tols []corev1.Toleration) bool {
	for i := range tols {
		t := tols[i]
		if t.Effect != "" && t.Effect != taint.Effect {
			continue
		}
		switch t.Operator {
		case corev1.TolerationOpEqual, "":
			if t.Key == taint.Key && t.Value == taint.Value {
				return true
			}
		case corev1.TolerationOpExists:
			if t.Key == taint.Key {
				return true
			}
		}
	}
	return false
}

func printMissingTolerations(m []missingTol) {
	for _, mt := range m {
		fmt.Printf("  missing toleration %s=%s:%s on nodes: %s\n",
			mt.Key, mt.Value, mt.Effect, strings.Join(mt.Nodes, ", "))
	}
}

func printTolerationSnippets(m []missingTol) {
	fmt.Println("\nSuggested tolerations to add (pick what fits your nodes):")
	for _, mt := range m {
		fmt.Printf(`- key: "%s"
  operator: "Equal"
  value: "%s"
  effect: "%s"
`, mt.Key, mt.Value, mt.Effect)
	}
}

// runKarpenterDiagnostics fetches logs from Karpenter controller pods and prints
// messages that are relevant to the pending pod. It looks for common reasons
// such as insufficient capacity, instance type constraints, AZ/zone constraints,
// disruptions, and provisioner/NodePool matching issues.
// runKarpenterDiagnostics returns the number of relevant messages found.
func runKarpenterDiagnostics(
    ctx context.Context,
    cli *kubernetes.Clientset,
    karpenterNamespace string,
    karpenterSelector string,
    targetNamespace string,
    targetPod string,
    since time.Duration,
    maxBytes int,
    printRaw bool,
) (int, error) {
    // find karpenter controller pods
    pods, err := cli.CoreV1().Pods(karpenterNamespace).List(ctx, metav1.ListOptions{LabelSelector: karpenterSelector})
    if err != nil {
        return 0, err
    }
    if len(pods.Items) == 0 {
        return 0, errors.New("no Karpenter controller pods found; adjust --karpenter-namespace/--karpenter-selector")
    }

    // build a list of streams to read
    deadline := time.Now().Add(-since)
    fmt.Printf("  reading logs since %s from %d controller pod(s)\n", deadline.Format(time.RFC3339), len(pods.Items))

    // try to match messages that mention the namespace/pod or common error patterns
    interesting := []string{
        targetPod,
        targetNamespace + "/" + targetPod,
        "insufficient capacity",
        "insufficient capacity error",
        "did not match",
        "no matching",
        "Unable to schedule",
        "Scheduling failed",
        "Insufficient vCPU",
        "Insufficient memory",
        "launch template",
        "instance type",
        "consolidation",
        "topology",
        "zonal",
        "availability zone",
        "provisioner",
        "NodePool",
        "DaemonSet overhead",
    }

    type hit struct {
        pod   string
        line  string
    }
    var hits []hit

    for i := range pods.Items {
        p := pods.Items[i]
        req := cli.CoreV1().Pods(karpenterNamespace).GetLogs(p.Name, &corev1.PodLogOptions{
            SinceTime: &metav1.Time{Time: deadline},
        })
        rc, err := req.Stream(ctx)
        if err != nil {
            // attempt fallback without SinceTime if kube doesn't support it
            req = cli.CoreV1().Pods(karpenterNamespace).GetLogs(p.Name, &corev1.PodLogOptions{})
            rc, err = req.Stream(ctx)
        }
        if err != nil {
            fmt.Printf("  warn: cannot read logs from %s: %v\n", p.Name, err)
            continue
        }
        data, err := ioutil.ReadAll(io.LimitReader(rc, int64(maxBytes)))
        _ = rc.Close()
        if err != nil {
            fmt.Printf("  warn: cannot read logs from %s: %v\n", p.Name, err)
            continue
        }
        lines := strings.Split(string(data), "\n")
        for _, ln := range lines {
            l := strings.ToLower(ln)
            for _, k := range interesting {
                if k == "" {
                    continue
                }
                if strings.Contains(l, strings.ToLower(k)) {
                    hits = append(hits, hit{pod: p.Name, line: trim(ln, 240)})
                    break
                }
            }
        }
    }

    if len(hits) == 0 {
        fmt.Println("  no relevant messages found in Karpenter logs for the timeframe.")
        return 0, nil
    }

    if printRaw {
        fmt.Printf("  relevant messages (%d):\n", len(hits))
        for _, h := range hits {
            fmt.Printf("   - [%s] %s\n", h.pod, h.line)
        }
    } else {
        // concise summary by counting messages per pod and per unique reason substring
        byPod := make(map[string]int)
        for _, h := range hits { byPod[h.pod]++ }
        fmt.Printf("  relevant messages: %d (summarized)\n", len(hits))
        // show top 3 pods by count
        type kv struct{ k string; v int }
        var arr []kv
        for k, v := range byPod { arr = append(arr, kv{k,v}) }
        sort.Slice(arr, func(i,j int) bool { return arr[i].v > arr[j].v })
        if len(arr) > 3 { arr = arr[:3] }
        for _, e := range arr {
            fmt.Printf("   - controller pod %s: %d hits\n", e.k, e.v)
        }
    }
    return len(hits), nil
}

// runKarpenterDeepDive prints the target pod settings and summarizes provisioners/nodepools
// to help explain why scale-up might be blocked (e.g., limits, requirements).
func runKarpenterDeepDive(ctx context.Context, dyn dynamic.Interface, ns, pod string, onlyMatching bool) {
    // Print pod key settings
    fmt.Println("\n  pod settings summary:")
    // Fetch pod via dynamic client to show nodeSelector/tolerations
    podGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
    pobj, err := dyn.Resource(podGVR).Namespace(ns).Get(ctx, pod, metav1.GetOptions{})
    podNodeSelector := map[string]string{}
    var podTolerations []map[string]interface{}
    if err == nil {
        if spec, ok := pobj.Object["spec"].(map[string]interface{}); ok {
            // used when filtering nodepools
            if nsMap, ok := spec["nodeSelector"].(map[string]interface{}); ok && len(nsMap) > 0 {
                fmt.Println("  pod.nodeSelector:")
                // stable order
                var keys []string
                for k := range nsMap { keys = append(keys, k) }
                sort.Strings(keys)
                for _, k := range keys {
                    v, _ := nsMap[k].(string)
                    fmt.Printf("    - %s=%s\n", k, v)
                    podNodeSelector[k] = v
                }
            } else {
                fmt.Println("  pod.nodeSelector: <none>")
            }
            if tols, ok := spec["tolerations"].([]interface{}); ok && len(tols) > 0 {
                fmt.Printf("  pod.tolerations (%d):\n", len(tols))
                for _, tv := range tols {
                    m, _ := tv.(map[string]interface{})
                    key, _ := m["key"].(string)
                    op, _ := m["operator"].(string)
                    val, _ := m["value"].(string)
                    eff, _ := m["effect"].(string)
                    if op == "Exists" || (op == "" && val == "") {
                        if eff != "" { fmt.Printf("    - key=%s op=Exists effect=%s\n", key, eff) } else { fmt.Printf("    - key=%s op=Exists\n", key) }
                    } else {
                        if eff != "" { fmt.Printf("    - %s=%s:%s\n", key, val, eff) } else { fmt.Printf("    - %s=%s\n", key, val) }
                    }
                    podTolerations = append(podTolerations, m)
                }
            } else {
                fmt.Println("  pod.tolerations: <none>")
            }
        }
    }

    // Karpenter resources
    // v1alpha5 Provisioner (older), NodePool and NodeClass (newer). Try both.
    // We only list and summarize limits/requirements to avoid huge dumps.

    gks := []schema.GroupVersionResource{
        {Group: "karpenter.sh", Version: "v1", Resource: "nodepools"},
    }

    for _, gvr := range gks {
        list, err := dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
        if err != nil {
            continue
        }
        if len(list.Items) == 0 {
            continue
        }
        fmt.Printf("  %s found: %d\n", gvr.Resource, len(list.Items))
        
        var matchingNodePools []string
        var defaultNodePool string
        
        for _, it := range list.Items {
            meta := it.GetName()
            // extract known spec fields if present
            spec, ok := it.Object["spec"].(map[string]interface{})
            if !ok {
                fmt.Printf("   - %s (no spec)\n", meta)
                continue
            }
            // parse limits for cpu/memory (print once)
            var limitCPU, limitMem resource.Quantity
            if lim, ok := spec["limits"].(map[string]interface{}); ok && len(lim) > 0 {
                if v, ok := lim["cpu"].(string); ok && v != "" { limitCPU = resource.MustParse(v) }
                if v, ok := lim["memory"].(string); ok && v != "" { limitMem = resource.MustParse(v) }
            }
            // template with taints/labels
            var templateLabels map[string]string
            var templateTaints []map[string]interface{}
            if tmpl, ok := spec["template"].(map[string]interface{}); ok {
                if specMap, ok := tmpl["spec"].(map[string]interface{}); ok {
                    if taints, ok := specMap["taints"].([]interface{}); ok && len(taints) > 0 {
                        for _, tv := range taints {
                            m, _ := tv.(map[string]interface{})
                            templateTaints = append(templateTaints, m)
                        }
                    }
                    // labels can be under template.metadata.labels (v1 API) or template.spec.labels (legacy)
                    var lbls map[string]interface{}
                    if metaMap, ok := tmpl["metadata"].(map[string]interface{}); ok {
                        if l, ok := metaMap["labels"].(map[string]interface{}); ok {
                            lbls = l
                        }
                    }
                    if lbls == nil {
                        if l, ok := specMap["labels"].(map[string]interface{}); ok {
                            lbls = l
                        }
                    }
                    if lbls != nil && len(lbls) > 0 {
                        if templateLabels == nil { templateLabels = map[string]string{} }
                        for k, v := range lbls {
                            if vs, ok := v.(string); ok {
                                templateLabels[k] = vs
                            }
                        }
                    }
                }
            }

            // relevance filter: only show NodePools that the pod is targeting via tolerations
            if onlyMatching {
                relevant := false
                
                // Check if this NodePool is targeted by the pod's tolerations
                // Look for tolerations that match this NodePool's characteristics
                for _, pm := range podTolerations {
                    pk, _ := pm["key"].(string)
                    po, _ := pm["operator"].(string)
                    pv, _ := pm["value"].(string)
                    pe, _ := pm["effect"].(string)
                    
                    // Check if pod tolerates this NodePool's taints
                    if len(templateTaints) > 0 {
                        for _, t := range templateTaints {
                            tk, _ := t["key"].(string)
                            tv, _ := t["value"].(string)
                            te, _ := t["effect"].(string)
                            
                            if po == "Exists" || (po == "" && pv == "") {
                                if pk == tk && (pe == "" || pe == te) { 
                                    relevant = true
                                    break 
                                }
                            } else {
                                if pk == tk && pv == tv && (pe == "" || pe == te) { 
                                    relevant = true
                                    break 
                                }
                            }
                        }
                    }
                    
                    // Check if pod tolerations match NodePool template labels (e.g., app_group=cow)
                    if !relevant && len(templateLabels) > 0 {
                        for k, v := range templateLabels {
                            if pk == k && pv == v {
                                relevant = true
                                break
                            }
                        }
                    }
                    
                    // Special case: if pod tolerates app_group=cow, show the "cow" NodePool
                    // even if it has app_type=cow instead of app_group=cow
                    if !relevant && pk == "app_group" && pv == "cow" && meta == "cow" {
                        relevant = true
                    }
                    
                    if relevant {
                        break
                    }
                }
                
                if relevant {
                    matchingNodePools = append(matchingNodePools, meta)
                }
                
                // Track default NodePool as fallback
                if meta == "default" {
                    defaultNodePool = meta
                }
                
                if !relevant {
                    // skip non-matching nodepools
                    continue
                }
            }

            // Display NodePool details only for matching NodePools
            // limits
            if lim, ok := spec["limits"].(map[string]interface{}); ok && len(lim) > 0 {
                fmt.Printf("   - %s limits: %v\n", meta, lim)
            }
            // requirements
            if reqs, ok := spec["requirements"].([]interface{}); ok && len(reqs) > 0 {
                fmt.Printf("   - %s requirements count: %d\n", meta, len(reqs))
            }
            // disruption / consolidation
            if dis, ok := spec["disruption"].(map[string]interface{}); ok && len(dis) > 0 {
                fmt.Printf("   - %s disruption: %v\n", meta, dis)
            }
            // template taints
            if len(templateTaints) > 0 {
                fmt.Printf("   - %s template taints (%d):\n", meta, len(templateTaints))
                for _, t := range templateTaints {
                    key, _ := t["key"].(string)
                    value, _ := t["value"].(string)
                    effect, _ := t["effect"].(string)
                    if effect == "" {
                        fmt.Printf("       - %s=%s\n", key, value)
                    } else {
                        fmt.Printf("       - %s=%s:%s\n", key, value, effect)
                    }
                }
            }
            // template labels
            if len(templateLabels) > 0 {
                fmt.Printf("   - %s template labels (%d):\n", meta, len(templateLabels))
                var keys []string
                for k := range templateLabels { keys = append(keys, k) }
                sort.Strings(keys)
                for _, k := range keys {
                    v := templateLabels[k]
                    fmt.Printf("       - %s=%s\n", k, v)
                }
            }

            // capacity check: sum allocatable of nodes matching template labels
            if len(templateLabels) > 0 && (limitCPU.Value() > 0 || limitMem.Value() > 0) {
                // Build selector
                sel := klabels.SelectorFromSet(klabels.Set(templateLabels))
                // List nodes via dynamic core client
                // We need a typed client for allocatable, so use dyn against nodes GVR
                nodeGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
                nlist, err := dyn.Resource(nodeGVR).List(ctx, metav1.ListOptions{})
                if err == nil {
                    var sumCPU, sumMem resource.Quantity
                    for _, n := range nlist.Items {
                        labs := n.GetLabels()
                        if !sel.Matches(klabels.Set(labs)) { continue }
                        // allocatable
                        if status, ok := n.Object["status"].(map[string]interface{}); ok {
                            if alloc, ok := status["allocatable"].(map[string]interface{}); ok {
                                if c, ok := alloc["cpu"].(string); ok { sumCPU.Add(resource.MustParse(c)) }
                                if m, ok := alloc["memory"].(string); ok { sumMem.Add(resource.MustParse(m)) }
                            }
                        }
                    }
                    // print comparison if limits set
                    var cmp []string
                    if limitCPU.Value() > 0 { cmp = append(cmp, fmt.Sprintf("cpu %s/%s cores", formatCPU(sumCPU), formatCPU(limitCPU))) }
                    if limitMem.Value() > 0 { cmp = append(cmp, fmt.Sprintf("memory %s/%s", formatMemGi(sumMem), formatMemGi(limitMem))) }
                    if len(cmp) > 0 {
                        fmt.Printf("   - %s capacity vs limits: %s\n", meta, strings.Join(cmp, ", "))
                        // basic shortfall flags
                        if limitCPU.Value() > 0 && sumCPU.Cmp(limitCPU) < 0 {
                            diff := limitCPU.DeepCopy()
                            diff.Sub(sumCPU)
                            fmt.Printf("     ! cpu shortfall: need %s cores more\n", formatCPU(diff))
                        } else if limitCPU.Value() > 0 && sumCPU.Cmp(limitCPU) == 0 {
                            fmt.Printf("     ! cpu at limit: no headroom to scale further\n")
                        }
                        if limitMem.Value() > 0 && sumMem.Cmp(limitMem) < 0 {
                            diff := limitMem.DeepCopy()
                            diff.Sub(sumMem)
                            fmt.Printf("     ! memory shortfall: need %s more\n", formatMemGi(diff))
                        } else if limitMem.Value() > 0 && sumMem.Cmp(limitMem) == 0 {
                            fmt.Printf("     ! memory at limit: no headroom to scale further\n")
                        }
                    }
                }
            }
        }
        
        // Show summary of matching NodePools
        if onlyMatching {
            if len(matchingNodePools) > 0 {
                fmt.Printf("  ✅ Showing %d matching NodePool(s): %s\n", len(matchingNodePools), strings.Join(matchingNodePools, ", "))
            } else {
                fmt.Println("  ⚠️  No NodePools match the pod's requirements!")
                fmt.Println("     Pod nodeSelector:", podNodeSelector)
                fmt.Println("     Pod tolerations:", len(podTolerations), "entries")
                if defaultNodePool != "" {
                    fmt.Printf("     Showing default NodePool '%s' as fallback:\n", defaultNodePool)
                    // Re-run the loop to show only the default NodePool
                    for _, it := range list.Items {
                        if it.GetName() == defaultNodePool {
                            // Show default NodePool details (reuse existing logic)
                            meta := it.GetName()
                            spec, ok := it.Object["spec"].(map[string]interface{})
                            if !ok {
                                fmt.Printf("   - %s (no spec)\n", meta)
                                continue
                            }
                            // Show limits, disruption, taints, labels for default
                            var defaultLimitCPU, defaultLimitMem resource.Quantity
                            if lim, ok := spec["limits"].(map[string]interface{}); ok && len(lim) > 0 {
                                fmt.Printf("   - %s limits: %v\n", meta, lim)
                                if v, ok := lim["cpu"].(string); ok && v != "" { defaultLimitCPU = resource.MustParse(v) }
                                if v, ok := lim["memory"].(string); ok && v != "" { defaultLimitMem = resource.MustParse(v) }
                            } else {
                                fmt.Printf("   - %s limits: <none> (no capacity constraints)\n", meta)
                            }
                            if dis, ok := spec["disruption"].(map[string]interface{}); ok && len(dis) > 0 {
                                fmt.Printf("   - %s disruption: %v\n", meta, dis)
                            }
                            if tmpl, ok := spec["template"].(map[string]interface{}); ok {
                                if specMap, ok := tmpl["spec"].(map[string]interface{}); ok {
                                    if taints, ok := specMap["taints"].([]interface{}); ok && len(taints) > 0 {
                                        fmt.Printf("   - %s template taints (%d):\n", meta, len(taints))
                                        for _, tv := range taints {
                                            m, _ := tv.(map[string]interface{})
                                            key, _ := m["key"].(string)
                                            value, _ := m["value"].(string)
                                            effect, _ := m["effect"].(string)
                                            if effect == "" {
                                                fmt.Printf("       - %s=%s\n", key, value)
                                            } else {
                                                fmt.Printf("       - %s=%s:%s\n", key, value, effect)
                                            }
                                        }
                                    }
                                    var lbls map[string]interface{}
                                    if metaMap, ok := tmpl["metadata"].(map[string]interface{}); ok {
                                        if l, ok := metaMap["labels"].(map[string]interface{}); ok {
                                            lbls = l
                                        }
                                    }
                                    if lbls == nil {
                                        if l, ok := specMap["labels"].(map[string]interface{}); ok {
                                            lbls = l
                                        }
                                    }
                                    if lbls != nil && len(lbls) > 0 {
                                        fmt.Printf("   - %s template labels (%d):\n", meta, len(lbls))
                                        var keys []string
                                        for k := range lbls { keys = append(keys, k) }
                                        sort.Strings(keys)
                                        for _, k := range keys {
                                            v, _ := lbls[k].(string)
                                            fmt.Printf("       - %s=%s\n", k, v)
                                        }
                                        
                                        // Add capacity vs limits analysis for default NodePool
                                        if defaultLimitCPU.Value() > 0 || defaultLimitMem.Value() > 0 {
                                            // Build selector for default NodePool
                                            defaultTemplateLabels := make(map[string]string)
                                            for k, v := range lbls {
                                                if vs, ok := v.(string); ok {
                                                    defaultTemplateLabels[k] = vs
                                                }
                                            }
                                            sel := klabels.SelectorFromSet(klabels.Set(defaultTemplateLabels))
                                            
                                            // List nodes and sum allocatable
                                            nodeGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
                                            nlist, err := dyn.Resource(nodeGVR).List(ctx, metav1.ListOptions{})
                                            if err == nil {
                                                var sumCPU, sumMem resource.Quantity
                                                for _, n := range nlist.Items {
                                                    labs := n.GetLabels()
                                                    if !sel.Matches(klabels.Set(labs)) { continue }
                                                    // allocatable
                                                    if status, ok := n.Object["status"].(map[string]interface{}); ok {
                                                        if alloc, ok := status["allocatable"].(map[string]interface{}); ok {
                                                            if c, ok := alloc["cpu"].(string); ok { sumCPU.Add(resource.MustParse(c)) }
                                                            if m, ok := alloc["memory"].(string); ok { sumMem.Add(resource.MustParse(m)) }
                                                        }
                                                    }
                                                }
                                                // print comparison if limits set
                                                var cmp []string
                                                if defaultLimitCPU.Value() > 0 { cmp = append(cmp, fmt.Sprintf("cpu %s/%s cores", formatCPU(sumCPU), formatCPU(defaultLimitCPU))) }
                                                if defaultLimitMem.Value() > 0 { cmp = append(cmp, fmt.Sprintf("memory %s/%s", formatMemGi(sumMem), formatMemGi(defaultLimitMem))) }
                                                if len(cmp) > 0 {
                                                    fmt.Printf("   - %s capacity vs limits: %s\n", meta, strings.Join(cmp, ", "))
                                                    // basic shortfall flags
                                                    if defaultLimitCPU.Value() > 0 && sumCPU.Cmp(defaultLimitCPU) < 0 {
                                                        diff := defaultLimitCPU.DeepCopy()
                                                        diff.Sub(sumCPU)
                                                        fmt.Printf("     ! cpu shortfall: need %s cores more\n", formatCPU(diff))
                                                    } else if defaultLimitCPU.Value() > 0 && sumCPU.Cmp(defaultLimitCPU) == 0 {
                                                        fmt.Printf("     ! cpu at limit: no headroom to scale further\n")
                                                    }
                                                    if defaultLimitMem.Value() > 0 && sumMem.Cmp(defaultLimitMem) < 0 {
                                                        diff := defaultLimitMem.DeepCopy()
                                                        diff.Sub(sumMem)
                                                        fmt.Printf("     ! memory shortfall: need %s more\n", formatMemGi(diff))
                                                    } else if defaultLimitMem.Value() > 0 && sumMem.Cmp(defaultLimitMem) == 0 {
                                                        fmt.Printf("     ! memory at limit: no headroom to scale further\n")
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            break
                        }
                    }
                }
            }
        }
    }
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func age(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return time.Since(t).Round(time.Second).String()
}

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func printSelectorHint(pod *corev1.Pod) {
	if len(pod.Spec.NodeSelector) > 0 {
		fmt.Println("Pod has nodeSelector:", pod.Spec.NodeSelector)
		fmt.Println("Check if any nodes have matching labels.")
	} else {
		fmt.Println("Pod has no nodeSelector constraints.")
	}
}

// formatCPU returns cores with 2 decimal places, e.g., 63.35
func formatCPU(q resource.Quantity) string {
    mv := q.MilliValue()
    cores := float64(mv) / 1000.0
    return fmt.Sprintf("%.2f", cores)
}

// formatMemGi returns Gi with 2 decimal places, e.g., 238.30Gi
func formatMemGi(q resource.Quantity) string {
    const gi = 1024 * 1024 * 1024
    bytes := float64(q.Value())
    return fmt.Sprintf("%.2fGi", bytes/gi)
}
