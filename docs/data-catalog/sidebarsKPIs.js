const sidebars = {
  kpiSidebar: [
    {
      type: "doc",
      id: "overview", // your overview page
      label: "Overview",
    },
    {
      type: "category",
      label: "KPIs by Department",
      collapsible: true,
      collapsed: false,
      items: [
        { type: "doc", id: "sales", label: "Sales" },
        { type: "doc", id: "credit", label: "Credit" },
        { type: "doc", id: "supply-chain", label: "Supply Chain" },
        { type: "doc", id: "operations", label: "Operations" },
        { type: "doc", id: "aftersales", label: "Aftersales" },

        {
          type: "doc",
          id: "customer-experience",
          label: "Customer Experience",
        },
        {
          type: "doc",
          id: "business-development",
          label: "Business Development",
        },
        { type: "doc", id: "marketing", label: "Marketing" },
        { type: "doc", id: "finance", label: "Finance" },
        { type: "doc", id: "esg", label: "ESG" },
        { type: "doc", id: "carbon", label: "Carbon" },
        { type: "doc", id: "product", label: "Product" },
        { type: "doc", id: "people-culture", label: "People & Culture" },
      ],
    },
  ],
};

export default sidebars;
