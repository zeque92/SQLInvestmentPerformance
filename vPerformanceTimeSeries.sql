create or alter view [PerfTest].[vPerformanceTimeSeries] as 

--------------------------------------------------------------------------------------------------------------------------
-- This view calculates daily market values and cashflows for different structures, adding returns in currency, percentage and log percentage.
-- Market values, cashflows, and returns are reported in Portfolio Currency (PC) only.
--------------------------------------------------------------------------------------------------------------------------

	with struct as (

		-- calculate sums for each required level of structure
		select

			mv.PortfolioIK,
			max(mv.PortfolioShortName) as PortfolioShortName,  -- use of max() here is simply to make grouping by more convenient. Always only one value per PortfolioIK.
			max(mv.PortfolioCurrency) as PortfolioCurrency,
			s.StructureIK,
			s.LevelCnt,
			case
				when grouping(si.Level1) = 1 then 0
				when grouping(si.Level2) = 1 then 1
				when grouping(si.Level3) = 1 then 2
				when grouping(si.Level4) = 1 then 3
				else 4
			end as LevelNr,
			max(s.Level1) as Level1Name,  -- same here, only one value per StructureIK
			si.Level1,
			max(s.Level2) as Level2Name,
			si.Level2,
			max(s.Level3) as Level3Name,
			si.Level3,
			max(s.Level4) as Level4Name,
			si.Level4,
			mv.Todate,
			count(mv.InstrumentIK) as InstrumentCount,  -- not used, just for info
			sum(mv.MarketValuePC) as MarketValuePC,
			sum(mv.CashFlowPC) as CashFlowPC,
			sum(mv.lag_MarketValuePC) as lag_MarketValuePC

		from

			PerfTest.vMarketValueAndCashFlow mv
			inner join PerfTest.vStructureInstrument si with (noexpand) on si.InstrumentIK = mv.InstrumentIK 
			inner join PerfTest.Structure s on s.StructureIK = si.StructureIK

		group by grouping sets (

				-- use separate grouping sets for total portfolio and each of the structure levels to get all market values and cash flows summed up accordingly
				(
					mv.PortfolioIK,
					s.StructureIK,
					mv.Todate,
					s.LevelCnt
				), (
					mv.PortfolioIK,
					s.StructureIK,
					mv.Todate,
					s.LevelCnt,
					si.Level1
				), (
					mv.PortfolioIK,
					s.StructureIK,
					mv.Todate,
					s.LevelCnt,
					si.Level1,
					si.Level2
				), (
					mv.PortfolioIK,
					s.StructureIK,
					mv.Todate,
					s.LevelCnt,
					si.Level1,
					si.Level2,
					si.Level3
				), (
					mv.PortfolioIK,
					s.StructureIK,
					mv.Todate,
					s.LevelCnt,
					si.Level1,
					si.Level2,
					si.Level3,
					si.Level3,
					si.Level4
				)
			)
		
		having
			-- filter out grouping sets not needed (e.g. where structure stops at level 3, do not include level 4)
			-- if the level is grouped (i.e. grouping() = 1) or the level is active in structure (LevelCnt is at least this level), then show data
			-- level 1 is always included
			(grouping(si.Level2) = 1 or s.LevelCnt >= 2)
			and (grouping(si.Level3) = 1 or s.LevelCnt >= 3)
			and (grouping(si.Level4) = 1 or s.LevelCnt >= 4)
				
		)

	-- add return calculations to the above data
	select
		s.*,
		a.*

	from
		struct s
		cross apply (
			-- calculate returns
			select
				-- simple return in currency
				s.MarketValuePC - s.CashFlowPC - s.lag_MarketValuePC as ReturnPC,
				-- return percentages - normally assume cashflow happens at end of day, but if cashflow is very large relative to market value --> assume start of day (e.g. opening a position)
				-- if market value is negative either end of day or start of day, do not try to present a % return (output: null)
				-- convert to float for maximum accuracy, calculate both a "normal" percentage return and a natural logarithm percentage return
				case
					when s.lag_MarketValuePC < 0 or s.MarketValuePC < 0 then cast(null as float)  
					when s.CashFlowPC / nullif(s.MarketValuePC, 0) > 0.8 then convert(float, s.MarketValuePC) / nullif(s.lag_MarketValuePC + s.CashFlowPC, 0) - 1 
					else convert(float, s.MarketValuePC - s.CashFlowPC) / nullif(s.lag_MarketValuePC, 0) - 1 
				end as ReturnPCPerc,
				case
					when s.lag_MarketValuePC < 0 or s.MarketValuePC < 0 then cast(null as float)
					when s.CashFlowPC / nullif(s.MarketValuePC, 0) > 0.8 then log(convert(float, s.MarketValuePC) / nullif(s.lag_MarketValuePC + s.CashFlowPC, 0))
					else log(convert(float, nullif(s.MarketValuePC - s.CashFlowPC, 0)) / nullif(s.lag_MarketValuePC, 0))
				end as ReturnPCLog
		) a


GO


