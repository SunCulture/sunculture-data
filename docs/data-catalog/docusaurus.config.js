import { themes as prismThemes } from "prism-react-renderer";

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "SunCulture",
  tagline: "Data Catalog",
  trailingSlash: true,

  url: "https://sunculture.github.io",
  baseUrl: "/sunculture-data/",

  organizationName: "SunCulture",
  projectName: "sunculture-data",

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          path: "docs", // You can just put an empty file here to silence the plugin
          routeBasePath: "/", // If you prefer, you can also set this to something else
          sidebarPath: false, // No sidebar needed
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ["rss", "atom"],
            xslt: true,
          },
          editUrl:
            "https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/",
          onInlineTags: "warn",
          onInlineAuthors: "warn",
          onUntruncatedBlogPosts: "warn",
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      },
    ],
  ],

  plugins: [
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "dashboards",
        path: "Dashboards",
        routeBasePath: "dashboards",
        sidebarPath: require.resolve("./sidebarsDashboards.js"),
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "insights",
        path: "Insights",
        routeBasePath: "insights",
        sidebarPath: require.resolve("./sidebarsInsights.js"),
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "kpis",
        path: "KPIsAndMetrics",
        routeBasePath: "kpis", // URL will be like /kpis/overview
        sidebarPath: require.resolve("./sidebarsKPIs.js"),
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "datasources",
        path: "DataSources",
        routeBasePath: "data-sources",
        sidebarPath: require.resolve("./sidebarsDataSources.js"),
      },
    ],
  ],

  themes: [
    [
      require.resolve("@easyops-cn/docusaurus-search-local"),
      {
        hashed: true,
        language: ["en"],
        indexDocs: true,
        indexPages: true,
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],

  themeConfig: {
    image: "img/docusaurus-social-card.jpg",
    navbar: {
      logo: {
        alt: "SunCulture Logo",
        src: "img/SunCulture_Logo.png",
        style: {
          height: "40px",
          width: "auto",
        },
      },
      items: [
        {
          to: "/kpis/overview", // new routeBasePath
          label: "KPIs & Metrics",
          position: "left",
        },
        {
          to: "/dashboards/overview",
          label: "Dashboards",
          position: "left",
        },
        {
          to: "/data-sources/overview",
          label: "Data Sources",
          position: "left",
        },
        {
          to: "/insights/overview",
          label: "Insights",
          position: "left",
        },
        {
          href: "https://sunculture.io/",
          label: "SunCulture",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [],
      copyright: `Copyright © ${new Date().getFullYear()} SunCulture Data, Inc.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  },
};

export default config;
