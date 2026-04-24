import { source } from "@/lib/source";

// /llms.txt: index-style file recommended by llmstxt.org for
// LLMs that want a compact map of the site. Each doc page appears
// as a bullet with a short description and a link to the raw
// markdown version at /docs-md/<slug>.
//
// For the full-text concatenation see /llms-full.txt.
export const revalidate = false;

export function GET() {
  const pages = source.getPages();
  const lines: string[] = [];
  lines.push("# golars");
  lines.push("");
  lines.push(
    "golars is a pure-Go DataFrame library modeled on polars, built on arrow-go. Eager and lazy execution, streaming engine, SIMD kernels, no cgo.",
  );
  lines.push("");
  lines.push("## Documentation");
  lines.push("");
  for (const page of pages) {
    const slug = page.slugs.join("/") || "index";
    const url = `/docs-md/${slug}`;
    const title = page.data.title;
    const desc = page.data.description ?? "";
    lines.push(`- [${title}](${url}): ${desc}`);
  }
  lines.push("");
  lines.push("## Reference");
  lines.push("");
  lines.push(
    "- [llms-full.txt](/llms-full.txt): every page concatenated for single-request ingestion.",
  );
  lines.push(
    "- [GitHub](https://github.com/Gaurav-Gosain/golars): source, issues, releases.",
  );
  return new Response(lines.join("\n"), {
    status: 200,
    headers: { "content-type": "text/markdown; charset=utf-8" },
  });
}
