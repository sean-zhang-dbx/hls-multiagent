"""Patch the e2e-chatbot-app-next frontend to add HLS resource links to the sidebar."""

import re
from pathlib import Path

WORKSPACE = "https://fevm-sean-zhang.cloud.databricks.com"

RESOURCE_LINKS_COMPONENT = '''
function ResourceLinks() {
  const { open } = useSidebar();
  if (!open) return null;

  const links = [
    {
      label: "Genie: Cancer Incidence",
      href: "WORKSPACE_URL/genie/rooms/01f117bb5fff15c58a5541490e9d6660",
      icon: "🔬",
    },
    {
      label: "Genie: Clinical Trials",
      href: "WORKSPACE_URL/genie/rooms/01f117bb603f10539cb59a20ea08c344",
      icon: "💊",
    },
    {
      label: "Vector Search (ZINC)",
      href: "WORKSPACE_URL/compute/vector-search/hls-agent-vs-endpoint",
      icon: "🧪",
    },
    {
      label: "MLflow Experiment",
      href: "WORKSPACE_URL/ml/experiments/2778446027764261",
      icon: "📊",
    },
    {
      label: "Lakebase Memory",
      href: "WORKSPACE_URL/lakebase/projects/3d438ae3-84ba-4965-b92a-7ef27ccfae97",
      icon: "🧠",
    },
    {
      label: "UC: Clinical Alert",
      href: "WORKSPACE_URL/explore/data/functions/sean_zhang_catalog/gsk_india_hls/send_clinical_alert?o=7474654216647582",
      icon: "🚨",
    },
    {
      label: "UC: Web Search",
      href: "WORKSPACE_URL/explore/data/functions/sean_zhang_catalog/gsk_india_hls/tavily_web_search?o=7474654216647582",
      icon: "🌐",
    },
  ];

  return (
    <div className="px-3 py-2">
      <p className="mb-1.5 px-1 text-[11px] font-medium uppercase tracking-wider text-muted-foreground/60">
        Resources
      </p>
      <div className="space-y-0.5">
        {links.map((link) => (
          <a
            key={link.label}
            href={link.href}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 rounded-md px-1.5 py-1 text-xs text-muted-foreground hover:bg-accent hover:text-foreground transition-colors"
          >
            <span className="text-sm leading-none">{link.icon}</span>
            <span className="truncate">{link.label}</span>
          </a>
        ))}
      </div>
    </div>
  );
}
'''.replace("WORKSPACE_URL", WORKSPACE)

SIDEBAR_TITLE_REPLACEMENT = '''<span className="text-base font-semibold text-foreground">
              HLS Research Agent
            </span>'''


def patch_sidebar(frontend_dir: Path) -> bool:
    sidebar_path = frontend_dir / "client" / "src" / "components" / "app-sidebar.tsx"
    if not sidebar_path.exists():
        print(f"[patch] Sidebar not found: {sidebar_path}")
        return False

    content = sidebar_path.read_text()

    if "ResourceLinks" in content:
        print("[patch] Sidebar already patched, skipping.")
        return True

    content = content.replace(
        '<span className="text-base font-semibold text-foreground">\n              Chatbot\n            </span>',
        SIDEBAR_TITLE_REPLACEMENT,
    )

    insert_marker = "{/* ── Chat history"
    if insert_marker not in content:
        print(f"[patch] Could not find insertion marker in sidebar")
        return False

    resource_section = '''      {/* ── Resource Links ──────────────────────────────────────────── */}
      {effectiveOpen && <ResourceLinks />}
      <div className="mx-3 border-t border-border" />

'''
    content = content.replace(
        insert_marker,
        resource_section + "      " + insert_marker,
    )

    closing_marker = "\nexport function AppSidebar"
    content = content.replace(
        closing_marker,
        "\n" + RESOURCE_LINKS_COMPONENT + "\n" + "export function AppSidebar",
    )

    sidebar_path.write_text(content)
    print("[patch] Sidebar patched successfully with resource links.")
    return True


def patch_greeting(frontend_dir: Path) -> bool:
    greeting_path = frontend_dir / "client" / "src" / "components" / "greeting.tsx"
    if not greeting_path.exists():
        print(f"[patch] Greeting not found: {greeting_path}")
        return False

    content = greeting_path.read_text()

    if "HLS Research" in content:
        print("[patch] Greeting already patched, skipping.")
        return True

    content = re.sub(
        r"How can I help you today\?",
        "Ask me about molecules, clinical trials, cancer data, or the latest research.",
        content,
    )

    greeting_path.write_text(content)
    print("[patch] Greeting patched successfully.")
    return True


def main():
    frontend_dir = Path("e2e-chatbot-app-next")
    if not frontend_dir.exists():
        print("[patch] Frontend directory not found. Run after clone.")
        return

    patch_sidebar(frontend_dir)
    patch_greeting(frontend_dir)


if __name__ == "__main__":
    main()
