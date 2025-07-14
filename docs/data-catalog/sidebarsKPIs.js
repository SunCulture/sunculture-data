const sidebars = {
  kpiSidebar: [
    {
      type: "doc",
      id: "overview",
      label: "Overview",
    },
    {
      type: "category",
      label: "Metrics",
      collapsible: true,
      collapsed: false,
      items: [
        {
          type: "category",
          label: "Operations",
          items: [
            { type: "doc", id: "Operations/sales", label: "Sales" },
            { type: "doc", id: "Operations/credit", label: "Credit" },
            {
              type: "doc",
              id: "Operations/supply-chain",
              label: "Operations/Supply Chain",
            },
            { type: "doc", id: "Operations/operations", label: "Operations" },
            { type: "doc", id: "Operations/carbon", label: "Carbon" },
            { type: "doc", id: "Operations/aftersales", label: "After Sales" },
            {
              type: "doc",
              id: "Operations/customer-experience",
              label: "Customer Experience",
            },
            {
              type: "doc",
              id: "Operations/marketing",
              label: "Marketing",
            },
          ],
        },
        {
          type: "category",
          label: "Growth",
          items: [
            {
              type: "doc",
              id: "Growth/business-development",
              label: "Business Development",
            },
          ],
        },
        {
          type: "category",
          label: "Finance",
          items: [
            { type: "doc", id: "Finance/finance", label: "Finance" },
            { type: "doc", id: "Finance/esg", label: "ESG" },
          ],
        },
        {
          type: "category",
          label: "Product",
          items: [
            { type: "doc", id: "Product/product", label: "Product" },
            { type: "doc", id: "Product/engineering", label: "Engineering" },
            { type: "doc", id: "Product/data", label: "Data" },
            { type: "doc", id: "Product/tech-ops", label: "Product Ops" },
            { type: "doc", id: "Product/quality", label: "Quality" },
          ],
        },
        {
          type: "category",
          label: "People & Culture",
          items: [
            {
              type: "doc",
              id: "PeopleAndCulture/people-culture",
              label: "People & Culture",
            },
          ],
        },
        {
          type: "category",
          label: "External Affairs",
          items: [
            {
              type: "doc",
              id: "ExternalAffairs/external-affairs",
              label: "External Affairs",
            },
          ],
        },
      ],
    },
  ],
};

export default sidebars;
