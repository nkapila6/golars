"use client";

import Link from "next/link";
import { useState } from "react";

export default function HomePage() {
  return (
    <div className="container mx-auto px-4 py-16 md:py-24">
      {/* Hero Section */}
      <div className="text-center max-w-4xl mx-auto mb-16">
        <div className="inline-block px-3 py-1 mb-6 text-xs font-semibold rounded-full bg-fd-primary/10 text-fd-primary border border-fd-primary/20 animate-fadeInUp">
          Pure-Go DataFrames, polars-style
        </div>

        <div className="mb-6 animate-fadeInUp stagger-1" style={{ opacity: 0 }}>
          <img
            src="/golars.png"
            alt="golars"
            className="mx-auto w-full max-w-[720px] h-auto rounded-xl shadow-2xl"
          />
        </div>

        <p
          className="text-base md:text-xl text-fd-muted-foreground mb-8 leading-relaxed animate-fadeInUp stagger-2"
          style={{ opacity: 0 }}
        >
          Eager + lazy execution. Optimiser, streaming engine, SIMD kernels.
          <br />
          Matches or beats polars 1.39 on most polars-compare workloads. No cgo.
        </p>

        <div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center animate-fadeInUp stagger-4"
          style={{ opacity: 0 }}
        >
          <Link
            href="/docs/getting-started"
            className="btn-primary px-6 py-3 rounded-lg bg-fd-primary text-fd-primary-foreground font-semibold text-base shadow-lg"
          >
            Get Started →
          </Link>
          <a
            href="https://github.com/Gaurav-Gosain/golars"
            target="_blank"
            rel="noopener noreferrer"
            className="btn-secondary px-6 py-3 rounded-lg border-2 border-fd-border text-fd-foreground font-semibold text-base"
          >
            View on GitHub
          </a>
        </div>
      </div>

      {/* Features Grid */}
      <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6 max-w-6xl mx-auto mb-16">
        <FeatureCard
          icon="lightning"
          title="Eager + Lazy"
          description="Build pipelines as logical plans. Optimiser fuses projections, pushes filters, collapses scans. Then Collect."
        />
        <FeatureCard
          icon="workflow"
          title="Streaming engine"
          description="Morsel-driven execution for datasets that don't fit in memory. stream.New(cfg, source, stages, sink)."
        />
        <FeatureCard
          icon="layout-grid"
          title="polars-grade performance"
          description="SIMD kernels on amd64 via GOEXPERIMENT=simd. AVX2 VPBLENDVB blend, SIMD compare + arithmetic."
        />
        <FeatureCard
          icon="file-code"
          title=".glr scripting"
          description="One command per line. Drive the REPL, run from disk, embed from your own tools."
        />
        <FeatureCard
          icon="terminal"
          title="Rich CLI"
          description="golars run, fmt, lint, sql, schema, head, diff, browse, explain, completion."
        />
        <FeatureCard
          icon="scroll-text"
          title="LLM-native"
          description="MCP server exposes schema, head, describe, sql, diff to Claude / Cursor / Windsurf."
        />
      </div>

      {/* Quick Example */}
      <div className="max-w-4xl mx-auto">
        <h2 className="text-3xl font-bold text-center mb-8">Quick Start</h2>
        <div className="rounded-xl border-2 border-fd-border bg-fd-card p-6 md:p-8 shadow-xl">
          <div className="space-y-4">
            <TerminalBlock command="go get github.com/Gaurav-Gosain/golars@latest">
              <span className="text-fd-muted-foreground">
                # Add to your module
              </span>
              {"\n"}
              <span style={{ color: "hsl(var(--primary))" }}>go</span>
              {" get "}
              <span
                className="golars-glow font-bold"
                style={{ color: "#bb9af7" }}
              >
                github.com/Gaurav-Gosain/golars
              </span>
              @latest
            </TerminalBlock>
            <TerminalBlock command="golars sql 'SELECT * FROM trades WHERE volume > 100' trades.csv">
              <span className="text-fd-muted-foreground"># Query any file</span>
              {"\n"}
              <span
                className="golars-glow font-bold"
                style={{ color: "#bb9af7" }}
              >
                golars
              </span>
              {" sql "}
              <span style={{ color: "#9ece6a" }}>
                'SELECT * FROM trades WHERE volume &gt; 100'
              </span>
              {" trades.csv"}
            </TerminalBlock>
            <div className="pt-4 border-t border-fd-border">
              <p className="text-sm text-fd-muted-foreground mb-2">
                Essential commands:
              </p>
              <div className="grid sm:grid-cols-2 gap-2 text-sm">
                <div className="flex items-center gap-2">
                  <kbd>golars schema</kbd>
                  <span className="text-fd-muted-foreground">
                    → column names + dtypes
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <kbd>golars stats</kbd>
                  <span className="text-fd-muted-foreground">
                    → describe() summary
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <kbd>golars browse</kbd>
                  <span className="text-fd-muted-foreground">→ TUI viewer</span>
                </div>
                <div className="flex items-center gap-2">
                  <kbd>golars repl</kbd>
                  <span className="text-fd-muted-foreground">
                    → interactive .glr
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Stats */}
      <div className="mt-16 text-center">
        <div className="inline-flex flex-wrap gap-6 justify-center text-sm text-fd-muted-foreground">
          <Stat label="MIT License" />
          <Stat label="Pure Go (no cgo)" />
          <Stat label="Arrow-native" />
          <Stat label="Polars-compatible" />
        </div>
      </div>
    </div>
  );
}

function FeatureCard({
  icon,
  title,
  description,
}: {
  icon: string;
  title: string;
  description: string;
}) {
  const iconMap: Record<string, string> = {
    terminal:
      '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 17 10 11 4 5"/><line x1="12" x2="20" y1="19" y2="19"/></svg>',
    "layout-grid":
      '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="7" height="7" x="3" y="3" rx="1"/><rect width="7" height="7" x="14" y="3" rx="1"/><rect width="7" height="7" x="14" y="14" rx="1"/><rect width="7" height="7" x="3" y="14" rx="1"/></svg>',
    lightning:
      '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2 3 14h9l-1 8 10-12h-9l1-8z"/></svg>',
    workflow:
      '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="8" height="8" x="3" y="3" rx="2"/><path d="M7 11v4a2 2 0 0 0 2 2h4"/><rect width="8" height="8" x="13" y="13" rx="2"/></svg>',
    "scroll-text":
      '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 21h12a2 2 0 0 0 2-2v-2H10v2a2 2 0 1 1-4 0V5a2 2 0 1 0-4 0v3h4"/><path d="M19 17V5a2 2 0 0 0-2-2H4"/><path d="M15 8h-5"/><path d="M15 12h-5"/></svg>',
    "file-code":
      '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 12.5 8 15l2 2.5"/><path d="m14 12.5 2 2.5-2 2.5"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7z"/></svg>',
  };

  return (
    <div className="feature-card rounded-lg border border-fd-border bg-fd-card p-6 hover:border-fd-primary/50 transition-all">
      <div
        className="text-fd-primary mb-3 w-6 h-6"
        dangerouslySetInnerHTML={{ __html: iconMap[icon] || "" }}
      />
      <h3 className="font-semibold text-lg mb-2">{title}</h3>
      <p className="text-sm text-fd-muted-foreground leading-relaxed">
        {description}
      </p>
    </div>
  );
}

function Stat({ label }: { label: string }) {
  return <span className="font-mono">{label}</span>;
}


function TerminalBlock({
  command,
  children,
}: {
  command: string;
  children: React.ReactNode;
}) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(command);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group bg-fd-muted/50 backdrop-blur rounded-lg border border-fd-border/50 hover:border-fd-primary/30 transition-all overflow-hidden">
      {/* Terminal header */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-fd-border/50 bg-fd-muted/30">
        <div className="flex gap-1.5">
          <div className="w-3 h-3 rounded-full bg-red-500/80"></div>
          <div className="w-3 h-3 rounded-full bg-yellow-500/80"></div>
          <div className="w-3 h-3 rounded-full bg-green-500/80"></div>
        </div>
        <span className="text-xs text-fd-muted-foreground ml-2">bash</span>
      </div>
      {/* Terminal content */}
      <div className="relative">
        <pre className="p-4 overflow-x-auto">
          <code className="font-mono text-sm">{children}</code>
        </pre>
        {/* Copy button */}
        <button
          onClick={handleCopy}
          className="absolute top-2 right-2 p-2 rounded-md bg-fd-accent/50 border border-fd-border/50 hover:bg-fd-accent hover:border-fd-primary/50 transition-all opacity-0 group-hover:opacity-100"
          aria-label="Copy command"
        >
          {copied ? (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="text-green-500"
            >
              <polyline points="20 6 9 17 4 12"></polyline>
            </svg>
          ) : (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="text-fd-muted-foreground"
            >
              <rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect>
              <path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path>
            </svg>
          )}
        </button>
      </div>
    </div>
  );
}
