const sidebars = {
  kpiSidebar: [
    {
      type: "doc",
      id: "overview", // your overview page
      label: "Overview",
    },
    {
      type: "category",
      label: "Country Dashboards",
      collapsible: true,
      collapsed: false,
      items: [
        {
          type: "doc",
          id: "suncultureDashboardGlobal",
          label: "SunCulture Global",
        },
        {
          type: "doc",
          id: "suncultureDashboardKe",
          label: "SunCulture - Kenya",
        },
        { type: "doc", id: "suncultureDashboardUg", label: "SunCulture - Ug" },
        { type: "doc", id: "suncultureDashboardCIV", label: "Côte d'Ivoire" },
      ],
    },
  ],
};

export default sidebars;
