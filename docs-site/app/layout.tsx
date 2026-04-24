import { RootProvider } from 'fumadocs-ui/provider/next';
import './global.css';
import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: {
    template: '%s | golars',
    default: 'golars: pure-Go DataFrames',
  },
  description:
    'golars is a pure-Go port of polars on top of arrow-go. Eager and lazy execution, streaming, SIMD kernels, matches or beats polars 1.39 on most polars-compare workloads.',
  metadataBase: new URL('https://golars.gaurav.zip'),
  openGraph: {
    title: 'golars: pure-Go DataFrames',
    description:
      'A pure-Go DataFrame library modeled on polars, built directly on arrow-go. Lazy plan + optimizer, streaming engine, SIMD, no cgo.',
    url: 'https://golars.gaurav.zip',
    siteName: 'golars',
    type: 'website',
    images: [
      {
        url: '/golars.png',
        width: 1448,
        height: 1086,
        alt: 'golars: pure-Go DataFrames',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'golars: pure-Go DataFrames',
    description:
      'Polars-style DataFrames for Go: eager + lazy, streaming, SIMD, no cgo.',
    images: ['/golars.png'],
  },
  icons: {
    icon: '/golars.png',
    apple: '/golars.png',
  },
};

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        <RootProvider
          search={{
            options: {
              type: 'static',
            },
          }}
        >
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
