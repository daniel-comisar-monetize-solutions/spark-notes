SELECT DISTINCT
   saa.tenant
  ,saa.FacilityID
  ,saa.ItemID
  ,saa.StockAreaID
  ,saa.StockAreaName AS StockAreaName
  ,saa.StockAreaDescription AS StockAreaDescription
  ,saa.StockAreaTypeDescription AS StockAreaType
  ,saa.UnitCostInventoryValue AS StockAreaValue
  ,saa.InventoryLevel AS StockAreaTotalInv
  ,saa.ParLevel AS StockAreaParLevel
  ,saa.MaxInventory AS StockAreaMaxInv
  ,usage.ADU45 AS StockAreaADU
  ,usage.AWU45 AS StockAreaAWU
  ,usage.Peak45 AS StockAreaPeak
  ,saa.InventoryLevel/usage.ADU45 AS StockAreaDoH
  ,saa.InventoryLevel/usage.AWU45 AS StockAreaDoH_WU
  ,round(usage.TotalQuantity45*iuc.UnitCost,2) AS StockAreaUsageValue
  ,CASE WHEN 
      usage.TotalQuantity45 = 0 OR usage.TotalQuantity45 IS NULL
    THEN 'D'
    WHEN
        round(usage.TotalQuantity45*iuc.UnitCost,2)/ 
        SUM(round(usage.TotalQuantity45*iuc.UnitCost,2)) OVER(PARTITION BY saa.tenant, saa.stockareaid ORDER BY saa.tenant, saa.stockareaid) > .8     
    THEN 'A'
    WHEN
        SUM(round(usage.TotalQuantity45*iuc.UnitCost,2)) OVER(PARTITION BY saa.tenant, saa.stockareaid ORDER BY saa.tenant, saa.stockareaid, usage.TotalQuantity45*iuc.UnitCost DESC)/ 
        SUM(round(usage.TotalQuantity45*iuc.UnitCost,2)) OVER(PARTITION BY saa.tenant, saa.stockareaid ORDER BY saa.tenant, saa.stockareaid) <= .8 
    THEN 'A' 
    WHEN 
      SUM(round(usage.TotalQuantity45*iuc.UnitCost,2)) OVER(PARTITION BY saa.tenant, saa.stockareaid ORDER BY saa.tenant, saa.stockareaid, usage.TotalQuantity45*iuc.UnitCost DESC)/ 
      SUM(round(usage.TotalQuantity45*iuc.UnitCost,2)) OVER(PARTITION BY saa.tenant, saa.stockareaid ORDER BY saa.tenant, saa.stockareaid) <= .9
    THEN 'B' 
    ELSE 'C' END AS StockAreaABC,
    saa.earliestExpirationDttm,
    CASE WHEN
      DistinctCritcalGCNs.CriticalGCN IS NOT NULL
      THEN 'Y'
    ELSE 'N' END As CriticalMedFlag
FROM StockAreaAssignment saa
LEFT JOIN DailyUsageByStockArea AS usage ON usage.tenant = saa.tenant AND usage.FacilityId = saa.FacilityId AND usage.StockAreaId = saa.StockAreaId AND usage.ItemId = saa.ItemID
LEFT JOIN Customer cus ON cus.customerabbrv = saa.tenant
LEFT JOIN ItemUnitCost AS iuc ON usage.ItemId = iuc.ItemID and usage.tenant = iuc.tenant
LEFT JOIN ItemFacilityReadModel As ifrm ON ifrm.ItemId = saa.ItemId
LEFT JOIN (SELECT DISTINCT CriticalGCN FROM CriticalMedList) As DistinctCritcalGCNs ON DistinctCritcalGCNs.CriticalGCN = ifrm.gcn;
