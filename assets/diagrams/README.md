# Diagram assets (Mermaid → SVG)

Sources live in [`mmd/`](mmd/); rendered previews for README and docs are in [`svg/`](svg/).

## Prerequisites

- [Node.js](https://nodejs.org/) with `npx`, or a global install of `@mermaid-js/mermaid-cli` (`mmdc` on `PATH`).

## Regenerate one diagram

From the **repository root** (`herbatka/`):

```bash
npx -y @mermaid-js/mermaid-cli -i assets/diagrams/mmd/<name>.mmd -o assets/diagrams/svg/<name>.svg
```

PowerShell (repository root):

```powershell
npx -y @mermaid-js/mermaid-cli -i assets/diagrams/mmd/architecture-overview.mmd -o assets/diagrams/svg/architecture-overview.svg
```

## Regenerate all diagrams

PowerShell (repository root):

```powershell
$names = @(
  'architecture-overview',
  'fleet-ui-draft',
  'logical-channels-lanes',
  'logical-channels-topics',
  'persistence-recovery',
  'produce-path',
  'replay-segment-recovery',
  'request-flow',
  'simulator-load-flow',
  'source-of-truth-rebuild'
)
foreach ($n in $names) {
  npx -y @mermaid-js/mermaid-cli -i "assets/diagrams/mmd/$n.mmd" -o "assets/diagrams/svg/$n.svg"
}
```

Bash (repository root):

```bash
for n in architecture-overview fleet-ui-draft logical-channels-lanes logical-channels-topics \
  persistence-recovery produce-path replay-segment-recovery request-flow simulator-load-flow \
  source-of-truth-rebuild; do
  npx -y @mermaid-js/mermaid-cli -i "assets/diagrams/mmd/$n.mmd" -o "assets/diagrams/svg/$n.svg"
done
```

## Pairs (edit `.mmd`, then regenerate matching `.svg`)

| Source | Output |
|--------|--------|
| `mmd/architecture-overview.mmd` | `svg/architecture-overview.svg` |
| `mmd/fleet-ui-draft.mmd` | `svg/fleet-ui-draft.svg` |
| `mmd/logical-channels-lanes.mmd` | `svg/logical-channels-lanes.svg` |
| `mmd/logical-channels-topics.mmd` | `svg/logical-channels-topics.svg` |
| `mmd/persistence-recovery.mmd` | `svg/persistence-recovery.svg` |
| `mmd/produce-path.mmd` | `svg/produce-path.svg` |
| `mmd/replay-segment-recovery.mmd` | `svg/replay-segment-recovery.svg` |
| `mmd/request-flow.mmd` | `svg/request-flow.svg` |
| `mmd/simulator-load-flow.mmd` | `svg/simulator-load-flow.svg` |
| `mmd/source-of-truth-rebuild.mmd` | `svg/source-of-truth-rebuild.svg` |

Also documented from the repo root in [README.md](../../README.md) and [docs/reference/logical-channels.md](../../docs/reference/logical-channels.md).
