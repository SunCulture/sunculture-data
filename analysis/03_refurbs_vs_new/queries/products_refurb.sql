-- Product master from AMT with its refurb flag, joined to mart_accounts on productId = id.
-- FINAL: amt.products is a ReplacingMergeTree, so only the latest version of each product is kept.
-- companyRegionId: 1 = Kenya, 2 = CIV, 3 = Uganda, 4 = Global (amt.company_regions).
select distinct id,
    product,
    isRefurb,
    companyRegionId
from amt.products final
