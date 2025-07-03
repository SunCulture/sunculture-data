const sidebars = {
  datasourcesSidebar: [
    {
      type: "doc",
      id: "overview",
      label: "Overview",
    },
    {
      type: "category",
      label: "Internal Sources",
      items: ["InternalDataSources/internal-data-sources"],
    },
    {
      type: "category",
      label: "External Systems",
      items: ["ExternalDataSources/external-data-sources"],
    },
    {
      type: "category",
      label: "Manual Sources",
      items: ["ManualDataSources/manual-data-sources"],
    },
  ],
};

export default sidebars;
