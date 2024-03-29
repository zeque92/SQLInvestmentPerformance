create or alter function PerfTest.uftPerformanceTimeSeries(@portfolioik int, @bm_portfolioik int) returns table as

--------------------------------------------------------------------------------------------------------------------------
-- This inline table function combines Performance Time Series of two portfolios (a portfolio and a benchmark).
--
-- 2023-12-16   Kalle Saariaho     Initial release
--------------------------------------------------------------------------------------------------------------------------

return (

	select
		isnull(pf.StructureIK, bm.StructureIK) as StructureIK,
		isnull(pf.LevelCnt, bm.LevelCnt) as LevelCnt,
		isnull(pf.LevelNr, bm.LevelNr) as LevelNr,
		isnull(pf.Level1Name, bm.Level1Name) as Level1Name,  
		isnull(pf.Level1, bm.Level1) as Level1,
		isnull(pf.Level2Name, bm.Level2Name) as Level2Name,
		isnull(pf.Level2, bm.Level2) as Level2,
		isnull(pf.Level3Name, bm.Level3Name) as Level3Name,
		isnull(pf.Level3, bm.Level3) as Level3,
		isnull(pf.Level4Name, bm.Level4Name) as Level4Name,
		isnull(pf.Level4, bm.Level4) as Level4,
		isnull(pf.Todate, bm.Todate) as Todate,
		pf.MarketValuePC,
		pf.CashFlowPC,
		pf.lag_MarketValuePC,
		pf.ReturnPC,
		pf.ReturnPCPerc,
		pf.ReturnPCLog,
		bm.MarketValuePC as bm_MarketValuePC,
		bm.CashFlowPC as bm_CashFlowPC,
		bm.lag_MarketValuePC as bm_lag_MarketValuePC,
		bm.ReturnPC as bm_ReturnPC,
		bm.ReturnPCPerc as bm_ReturnPCPerc,
		bm.ReturnPCLog as bm_ReturnPCLog
	from
		(select * from PerfTest.vPerformanceTimeSeries where PortfolioIK = @portfolioik) pf
		full outer join
		(select * from PerfTest.vPerformanceTimeSeries where PortfolioIK = @bm_portfolioik) bm
		on	bm.StructureIK = pf.StructureIK
			and bm.LevelNr = pf.LevelNr
			and (bm.LevelNr <= 0 or bm.Level1 = pf.Level1)
			and (bm.LevelNr <= 1 or bm.Level2 = pf.Level2)
			and (bm.LevelNr <= 2 or bm.Level3 = pf.Level3)
			and (bm.LevelNr <= 3 or bm.Level4 = pf.Level4)
			and bm.Todate = pf.Todate

);