import { getLLMText, source } from "@/lib/source";
import { notFound } from "next/navigation";

// Raw-markdown route: every doc page is also served as plain
// markdown under /docs-md/<slug>. Intended for LLM hosts that want
// to ingest the docs without HTML noise.
//
// Mapping:
//   /docs/cookbook          -> /docs-md/cookbook
//   /docs/sql               -> /docs-md/sql
//
// The text includes the frontmatter title as an H1 so each page is
// self-contained when piped into an LLM context.
export const revalidate = false;

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ slug: string[] }> },
) {
  const { slug } = await params;
  const page = source.getPage(slug);
  if (!page) notFound();

  const text = await getLLMText(page);
  return new Response(text, {
    status: 200,
    headers: {
      "content-type": "text/markdown; charset=utf-8",
      "cache-control": "public, max-age=3600",
    },
  });
}

// Statically generate every slug so this route stays compatible
// with Next's `output: 'export'` build mode. The index page has an
// empty slug array which the catch-all route can't represent, so we
// filter it out; a dedicated `/docs-md/route.ts` serves that case.
export function generateStaticParams() {
  return source
    .getPages()
    .filter((page) => page.slugs.length > 0)
    .map((page) => ({
      slug: page.slugs,
    }));
}
