"""Utilities for Zipline admin CLI health checks."""

import json
import logging
import shutil
import subprocess
from urllib.parse import urlparse

from rich.table import Table

from ai.chronon.cli.theme import console

logger = logging.getLogger(__name__)

_KUBECTL_TIMEOUT = 10  # seconds


def _run_kubectl(args, timeout=_KUBECTL_TIMEOUT):
    """Run a kubectl subcommand with a timeout.

    Returns a CompletedProcess-like object; on TimeoutExpired, logs a warning
    and returns a namespace with returncode=1 and empty stdout/stderr so
    callers that check returncode behave correctly without special-casing.
    """
    cmd = ["kubectl"] + args
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("kubectl %s timed out after %ss", " ".join(args), timeout)

        class _TimedOut:
            returncode = 1
            stdout = ""
            stderr = f"timed out after {timeout}s"

        return _TimedOut()


def run_infra_checks():
    """Run Kubernetes infrastructure checks for Flink-on-EKS setup.

    Returns a list of (check_name, what, status, detail) tuples.
    """
    results = []

    if not shutil.which("kubectl"):
        results.append(
            (
                "kubectl",
                "kubectl binary present",
                "FAIL",
                "not found — install it and configure kubeconfig",
            )
        )
        return results
    results.append(("kubectl", "kubectl binary present", "ok", ""))

    r = _run_kubectl(["cluster-info"])
    if r.returncode != 0:
        results.append(
            (
                "cluster",
                "Can reach the Kubernetes API",
                "FAIL",
                r.stderr.strip() or "kubectl cluster-info failed",
            )
        )
        return results
    results.append(("cluster", "Can reach the Kubernetes API", "ok", ""))

    for ns in ("zipline-flink", "zipline-system"):
        r = _run_kubectl(["get", "namespace", ns])
        if r.returncode != 0:
            results.append(
                (
                    ns,
                    f"{ns} namespace exists",
                    "FAIL",
                    "not found — Terraform may not have been applied",
                )
            )
        else:
            results.append((ns, f"{ns} namespace exists", "ok", ""))

    r = _run_kubectl(["get", "role", "orchestration-flink-role", "-n", "zipline-flink"])
    if r.returncode != 0:
        results.append(
            (
                "orchestration-flink-role",
                "Flink RBAC role exists",
                "FAIL",
                "role missing — Terraform may not have been applied",
            )
        )
    else:
        results.append(("orchestration-flink-role", "Flink RBAC role exists", "ok", ""))

    r = _run_kubectl(
        [
            "auth",
            "can-i",
            "create",
            "flinkdeployments.flink.apache.org",
            "--namespace",
            "zipline-flink",
            "--as",
            "system:serviceaccount:zipline-system:orchestration-sa",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "yes":
        results.append(
            (
                "FlinkDeployment RBAC",
                "orchestration-sa can submit Flink jobs",
                "FAIL",
                "orchestration-sa lacks create permission on flinkdeployments",
            )
        )
    else:
        results.append(("FlinkDeployment RBAC", "orchestration-sa can submit Flink jobs", "ok", ""))

    r = _run_kubectl(
        [
            "auth",
            "can-i",
            "create",
            "ingresses.networking.k8s.io",
            "--namespace",
            "zipline-flink",
            "--as",
            "system:serviceaccount:zipline-system:orchestration-sa",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "yes":
        results.append(
            (
                "ingress RBAC",
                "orchestration-sa can manage Flink ingresses",
                "FAIL",
                "orchestration-sa lacks ingress permissions — check orchestration-flink-role",
            )
        )
    else:
        results.append(("ingress RBAC", "orchestration-sa can manage Flink ingresses", "ok", ""))

    r = _run_kubectl(["get", "serviceaccount", "zipline-flink-sa", "-n", "zipline-flink"])
    if r.returncode != 0:
        results.append(
            (
                "zipline-flink-sa",
                "Flink job service account exists",
                "FAIL",
                "SA missing — Terraform may not have been applied",
            )
        )
    else:
        results.append(("zipline-flink-sa", "Flink job service account exists", "ok", ""))

    r = _run_kubectl(["get", "crd", "flinkdeployments.flink.apache.org"])
    if r.returncode != 0:
        results.append(
            (
                "FlinkDeployment CRD",
                "Flink operator CRD installed",
                "FAIL",
                "CRD missing — flink-operator Helm chart or CRD manifest not applied",
            )
        )
    else:
        results.append(("FlinkDeployment CRD", "Flink operator CRD installed", "ok", ""))

    hub_base_url = None
    r = _run_kubectl(
        [
            "get",
            "deployment",
            "zipline-orchestration-hub",
            "-n",
            "zipline-system",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ]
    )
    if r.returncode != 0:
        results.append(
            (
                "HUB_BASE_URL",
                "Hub base URL is configured",
                "FAIL",
                "could not read hub deployment env vars",
            )
        )
    else:
        try:
            env_vars = json.loads(r.stdout)
            hub_base_url = next(
                (e.get("value") for e in env_vars if e.get("name") == "HUB_BASE_URL" and e.get("value") is not None),
                None,
            )
            if hub_base_url:
                results.append(("HUB_BASE_URL", "Hub base URL is configured", "ok", hub_base_url))
            else:
                results.append(
                    (
                        "HUB_BASE_URL",
                        "Hub base URL is configured",
                        "FAIL",
                        "not set — check hub_domain/hub_external_url vars or set-hub-base-url Job",
                    )
                )
        except (json.JSONDecodeError, StopIteration):
            results.append(
                (
                    "HUB_BASE_URL",
                    "Hub base URL is configured",
                    "FAIL",
                    "could not parse deployment env vars",
                )
            )

    if hub_base_url:
        hub_hostname = urlparse(hub_base_url).hostname or ""
        r = _run_kubectl(
            [
                "get",
                "ingress",
                "orchestration-hub-ingress",
                "-n",
                "zipline-system",
                "-o",
                "jsonpath={.spec.rules[0].host}",
            ]
        )
        if r.returncode != 0:
            results.append(
                (
                    "hub ingress",
                    "Hub ingress host matches HUB_BASE_URL",
                    "WARN",
                    "could not read ingress — may not exist yet",
                )
            )
        else:
            ingress_host = r.stdout.strip()
            if ingress_host == hub_hostname:
                results.append(
                    (
                        "hub ingress",
                        "Hub ingress host matches HUB_BASE_URL",
                        "ok",
                        ingress_host,
                    )
                )
            else:
                results.append(
                    (
                        "hub ingress",
                        "Hub ingress host matches HUB_BASE_URL",
                        "WARN",
                        f"ingress host {ingress_host!r} != HUB_BASE_URL hostname {hub_hostname!r}",
                    )
                )

        is_elb = ".elb." in hub_base_url and ".amazonaws.com" in hub_base_url
        if is_elb:
            r = _run_kubectl(
                [
                    "get",
                    "events",
                    "-n",
                    "zipline-system",
                    "--field-selector",
                    "reason=Completed",
                ]
            )
            if r.returncode == 0 and "set-hub-base-url" in r.stdout:
                results.append(
                    (
                        "set-hub-base-url",
                        "ELB hostname discovery job completed",
                        "ok",
                        "completed successfully",
                    )
                )
            else:
                results.append(
                    (
                        "set-hub-base-url",
                        "ELB hostname discovery job completed",
                        "WARN",
                        "no completion event — check: kubectl logs -n zipline-system -l job-name=set-hub-base-url",
                    )
                )

    return results


def print_check_table(title, results):
    """Render a diagnostics results table and exit 1 if any check FAILed."""
    table = Table(title=title, expand=False)
    table.add_column("Check", style="cyan", no_wrap=True)
    table.add_column("What", style="white")
    table.add_column("", width=1)
    table.add_column("Detail", style="white")

    all_ok = True
    for check, what, status, detail in results:
        if status == "ok":
            status_cell = "[green]✓[/green]"
        elif status == "WARN":
            status_cell = "[yellow]⚠[/yellow]"
        else:
            status_cell = "[red]✗[/red]"
            all_ok = False
        table.add_row(check, what, status_cell, detail)

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All checks passed.[/bold green]")
    else:
        console.print("\n[bold red]Some checks failed.[/bold red]")
        raise SystemExit(1)
