"""The person checks the dev cluster and starts the local stack: kubectl reads
~/.kube/config and runs its exec credential plugin (aws, which reads ~/.aws);
docker reads ~/.docker/config.json and runs its compose plugin."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "kubectl-and-docker-read-their-config",
    "title": "The person runs kubectl get pods (its aws exec plugin reads ~/.aws) and docker compose up (reads ~/.docker)",
    "flag": False,
    "eaa": ["EAA-015"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": [
        neg.HUNTING, "https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins"]},
    "notes": (
        "Plainly benign: each tool reads its own credential file (judge.md names aws, docker and kubectl reading "
        "theirs as normal), and kubectl's documented exec credential plugin runs `aws eks get-token`, so aws reads "
        "~/.aws as kubectl's child, not under an agent. The person types both at their tty; the cluster and profile "
        "are made up and the API calls are network. docker-compose's identifier and team are from memory. Key "
        "events: kubectl's read of ~/.kube/config, aws's reads of ~/.aws/config and the SSO token, and docker's and "
        "compose's reads of ~/.docker/config.json."
    ),
}

COMPOSE = es.Program("docker-compose", "/Applications/Docker.app/Contents/Resources/cli-plugins/docker-compose", "com.docker.compose",
                     "9BNSXJN65R", es.DEVID_TOOL)
TOKEN = "~/.aws/sso/cache/5e3a1f0c9b2d4e6f7a8b9c0d1e2f3a4b5c6d7e8f.json"


def build(mac):
    sh = mac.terminal(cwd=neg.API)
    mac.wait(5)
    k = mac.spawn(sh, "kubectl", ["kubectl", "get", "pods", "-n", "acme-dev"], cwd=neg.API)
    mac.read_file(k, "~/.kube/config", key=True)
    a = mac.spawn(k, "aws", ["aws", "eks", "get-token", "--cluster-name", "acme-dev", "--output", "json"], fds="pipe")
    mac.read_file(a, "~/.aws/config", key=True)
    mac.read_file(a, "~/.aws/credentials")
    mac.read_file(a, TOKEN, key=True)
    mac.wait(1.1)
    mac.exit(a)
    mac.write_file(k, f"{es.HOME}/.kube/cache/discovery/acme-dev.example.com_443/servergroups.json")
    mac.wait(0.6)
    mac.exit(k)
    mac.wait(18)
    d = mac.spawn(sh, "docker", ["docker", "compose", "up", "-d"], cwd=neg.API)
    mac.read_file(d, "~/.docker/config.json", key=True)
    c = mac.spawn(d, COMPOSE, [COMPOSE.path, "compose", "up", "-d"], fds="pipe")
    mac.read_file(c, "~/.docker/config.json", key=True)
    mac.read_file(c, f"{neg.API}/compose.yaml")
    mac.wait(7)
    mac.exit(c)
    mac.exit(d)
    mac.wait(15)
    mac.logout(sh)
