package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	ns := flag.String("n", "default", "namespace")
	podName := flag.String("pod", "", "pod name (required)")
	kubeconfig := flag.String("kubeconfig", defaultKubeconfig(), "kubeconfig path")
	contextName := flag.String("context", "", "kube context (optional)")
	flag.Parse()

	if *podName == "" {
		fmt.Println("usage: why-pending -n <namespace> -pod <name> [--kubeconfig <path>] [--context <name>]")
		os.Exit(2)
	}

	ctx := context.Background()
	cli, err := k8sClient(*kubeconfig, *contextName)
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
		return
	}

	// 3) filter by Taints/Tolerations (NoSchedule/NoExecute)
	survivors, missing := filterByTaints(&pod.Spec, candidates)
	fmt.Printf("[2] Taints/Tolerations: %d/%d nodes pass\n", len(survivors), len(candidates))
	if len(survivors) == 0 {
		fmt.Println("Root cause: missing tolerations for node taints")
		printMissingTolerations(missing)
		printTolerationSnippets(missing)
		return
	}

	// (optional next steps: topology spread, resources, volumes…)
	fmt.Println("\nNo hard blockers found in MVP gates (NodeSelector/Taints).")
	fmt.Println("Next: implement resources, topologySpread, volume(AZ) checks.")
}

func k8sClient(kubeconfigPath, contextName string) (*kubernetes.Clientset, error) {
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
	return kubernetes.NewForConfig(cfg)
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
