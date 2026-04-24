# AGENTS.md - docs-site

Agent brief for editing the golars documentation website.

## Stack

- **Fumadocs 16** on Next.js 16 (App Router), React 19, Tailwind v4, Biome, Bun.
- Static export (`output: 'export'` in `next.config.mjs`).
- Deploys to a static host; no server features beyond build-time generation.

## Running locally

```sh
bun install
bun run dev        # http://localhost:3000
bun run build      # static export -> ./out
bun run types:check
```

## Where content lives

- `content/docs/*.mdx` - doc pages. Each has a frontmatter block
  with `title`, `description`, and a `lucide-react` `icon` name.
- `content/docs/meta.json` - explicit ordering of the pages in
  the sidebar.

When you add a new page:

1. Create `content/docs/<slug>.mdx` with title + description + icon.
2. Add `"<slug>"` to `meta.json`.
3. The `.md` raw-markdown route picks up automatically.

## Components available in MDX

- `Callout`, `Card`, `Cards` from `fumadocs-ui/components/*` - import in the MDX.
- `Mermaid` from `@/components/mdx/mermaid` - charts.
- `Step`, `Steps` from `fumadocs-ui/components/steps` - registered in `mdx-components.tsx`.

## Rules

- No em-dashes. No emojis in copy (the hero's ASCII art is fine).
- Match the voice of the rest of the golars docs - plain, direct,
  no AI-slop phrasing.
- Mirror anything material between `/docs/*` and `~/dev/golars/docs/*`
  so `go doc` readers see the same content.

## LLM routes

- `/llms.txt` - index (short list of all doc pages + description).
- `/llms-full.txt` - every page concatenated.
- `/docs-md/<slug>` - raw markdown for a single page.
- When adding pages, none of these require touching - they
  auto-include new MDX files.
