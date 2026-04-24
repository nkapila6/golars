import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <>
          <span
            aria-hidden
            className="font-mono font-bold text-[#8B5CF6] text-lg"
          >
            ▓▓▓
          </span>
          <span className="font-semibold">golars</span>
        </>
      ),
    },
    githubUrl: 'https://github.com/Gaurav-Gosain/golars',
    links: [
      {
        text: 'Documentation',
        url: '/docs',
        active: 'nested-url',
      },
      {
        text: 'Cookbook',
        url: '/docs/cookbook',
        active: 'url',
      },
      {
        text: 'Reference',
        url: '/docs/api-surface',
        active: 'url',
      },
    ],
  };
}
