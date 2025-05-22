/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  dashboardsSidebar: [
    {
      type: "doc",
      id: "overview", // maps to Dashboards/overview.md
      label: "Dashboards Overview",
    },
    {
      type: "category",
      label: "SunCulture Global",
      items: ["global/overview"],
    },
    {
      type: "category",
      label: "SunCulture Dashboard - Ke",
      items: ["ke/overview"],
    },
    {
      type: "category",
      label: "SunCulture Dashboard - Ug",
      items: ["ug/overview"],
    },
    {
      type: "category",
      label: "SunCulture Dashboard - CIV",
      items: ["civ/overview"],
    },
  ],
};

export default sidebars;
