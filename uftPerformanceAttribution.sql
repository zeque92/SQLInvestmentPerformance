create or alter function PerfTest.uftPerformanceAttribution(@portfolioik int, @bm_portfolioik int) returns table as

--------------------------------------------------------------------------------------------------------------------------
-- This inline table function calculates daily Brinson-Fachler performance attribution and performance contribution using a structure.
--
-- All factors explain upper level (e.g. level 2 for the level 3 "items" below it) performance.
--
-- In [this definition of] attribution, when
--    - the upper level has no market value (percentage return is not defined), the effect will be shown as interaction
--    - the upper level has no benchmark return, the effect will be shown as allocation (by using portfolio return instead of benchmark return)
--    - the upper level has no benchmark weight (but has benchmark return), the interaction effect will be shown as selection
-- If the upper level return is not well defined (like derivatives subportfolios) the results may be strange (as may percentage returns being explained).
--
-- 2023-12-16   Kalle Saariaho     Initial release
--------------------------------------------------------------------------------------------------------------------------

return (

	select
		n.StructureIK,
		n.LevelCnt,
		n.LevelNr,
		n.Level1Name,
		n.Level1,
		n.Level2Name,
		n.Level2,
		n.Level3Name,
		n.Level3,
		n.Level4Name,
		n.Level4,
		n.Todate,

		n.MarketValuePC,
		d.Wgt,
		n.CashFlowPC,
		n.lag_MarketValuePC,
		d.lag_Wgt,
		n.ReturnPC,
		n.ReturnPCPerc,
		n.ReturnPCLog,

		up.MarketValuePC as up_MarketValuePC,
		up.CashFlowPC as up_CashFlowPC,
		up.lag_MarketValuePC as up_lag_MarketValuePC,
		up.ReturnPC as up_ReturnPC,
		up.ReturnPCPerc as up_ReturnPCPerc,
		up.ReturnPCLog as up_ReturnPCLog,

		n.bm_MarketValuePC,
		d.bm_Wgt,
		n.bm_CashFlowPC,
		n.bm_lag_MarketValuePC,
		d.bm_lag_Wgt,
		n.bm_ReturnPC,
		n.bm_ReturnPCPerc,
		n.bm_ReturnPCLog,

		up.bm_MarketValuePC as up_bm_MarketValuePC,
		up.bm_CashFlowPC as up_bm_CashFlowPC,
		up.bm_lag_MarketValuePC as up_bm_lag_MarketValuePC,
		up.bm_ReturnPC as up_bm_ReturnPC,
		up.bm_ReturnPCPerc as up_bm_ReturnPCPerc,
		up.bm_ReturnPCLog as up_bm_ReturnPCLog,

		c.ContribPerc,
		c.bm_ContribPerc,
		a.Allocation,
		a.Selection,
		a.Interaction

	from
		PerfTest.uftPerformanceTimeSeries(@portfolioik, @bm_portfolioik) n
		-- get upper level data for each portfolio item ("subportfolio", row)
		inner join PerfTest.uftPerformanceTimeSeries(@portfolioik, @bm_portfolioik) up 
			on	up.StructureIK = n.StructureIK 
				and up.Todate = n.Todate
				and (
					-- top level: use itself as upper node
					(up.LevelNr = 0 and n.LevelNr = 0) 
					-- other levels: upper level LevelNr = item LevelNr-1, and match all upper level identifiers
					or (
						n.LevelNr > 0
						and up.LevelNr = n.LevelNr - 1
						and (n.LevelNr <= 1 or up.Level1 = n.Level1)
						and (n.LevelNr <= 2 or up.Level2 = n.Level2)
						and (n.LevelNr <= 3 or up.Level3 = n.Level3)
					)
				)

		cross apply (
			-- convert two decimal numeric values to float for increased accuracy, calculate weight to upper level
			-- use isnull to replace null with zero, helps with later calculations
			select
				convert(float, isnull(n.MarketValuePC, 0)) as MarketValuePC,
				isnull(convert(float, n.MarketValuePC) / nullif(up.MarketValuePC, 0), 0) as Wgt,
				convert(float, isnull(n.lag_MarketValuePC, 0)) as lag_MarketValuePC,
				isnull(convert(float, n.lag_MarketValuePC) / nullif(up.lag_MarketValuePC, 0), 0) as lag_Wgt,
				convert(float, isnull(n.ReturnPC, 0)) as ReturnPC,
				isnull(n.ReturnPCPerc, 0) as ReturnPCPerc,
				convert(float, isnull(up.MarketValuePC, 0)) as up_MarketValuePC,
				convert(float, isnull(up.lag_MarketValuePC, 0)) as up_lag_MarketValuePC,
				convert(float, isnull(up.ReturnPC, 0)) as up_ReturnPC,
				isnull(up.ReturnPCPerc, 0) as up_ReturnPCPerc,
				convert(float, isnull(n.bm_MarketValuePC, 0)) as bm_MarketValuePC,
				isnull(convert(float, n.bm_MarketValuePC) / nullif(up.bm_MarketValuePC, 0), 0) as bm_Wgt,
				convert(float, isnull(n.bm_lag_MarketValuePC, 0)) as bm_lag_MarketValuePC,
				isnull(convert(float, n.bm_lag_MarketValuePC) / nullif(up.bm_lag_MarketValuePC, 0), 0) as bm_lag_Wgt,
				convert(float, isnull(n.bm_ReturnPC, 0)) as bm_ReturnPC,
				isnull(n.bm_ReturnPCPerc, 0) as bm_ReturnPCPerc,
				convert(float, isnull(up.bm_MarketValuePC, 0)) as up_bm_MarketValuePC,
				convert(float, isnull(up.bm_lag_MarketValuePC, 0)) as up_bm_lag_MarketValuePC,
				convert(float, isnull(up.bm_ReturnPC, 0)) as up_bm_ReturnPC,
				isnull(up.bm_ReturnPCPerc, 0) as up_bm_ReturnPCPerc
		) d				

		cross apply (
			-- calculate daily contribution to portfolio return % and benchmark return %
			select
				case
					-- scale daily returns: if upper level EUR return = 0 then upper level percentage return = 0 and contributions are item EUR return / upper level lagged market value (summing up to zero).
					when d.up_ReturnPC = 0 then d.ReturnPC / nullif(d.up_lag_MarketValuePC, 0)
					-- otherwise just scale the return by the item contribution to upper level EUR return, no need to calculate percentage returns as both have the same denominator
					else d.up_ReturnPCPerc * d.ReturnPC / d.up_ReturnPC
				end as ContribPerc,
				case
					when d.up_bm_ReturnPC = 0 then d.bm_ReturnPC / nullif(d.up_bm_lag_MarketValuePC, 0)
					else d.up_bm_ReturnPCPerc * d.bm_ReturnPC / d.up_bm_ReturnPC
				end as bm_ContribPerc
			) c

		cross apply (
			-- calculate daily attribution, matching upper level return difference
			-- using EUR returns (ReturnPC) and market values (lag_MarketValuePC) instead of weights and percentage returns to make calculations work better when up_lag_MarketValuePC is zero (e.g. derivative subportfolios).
			-- R = return (perc)	r = return (log)	W = Weight	U = upper level	p = portfolio	b = benchmark
			select
				-- allocation, by default: (Wp - Wb) * (Rb - RUb). 
				-- if Rp is defined (market value exists) but Rb is not: use Rp instead of Rb (will show effect as allocation instead of interaction) 
				(d.lag_Wgt - d.bm_lag_Wgt)
					* (case
						when d.lag_MarketValuePC <> 0.0 and d.bm_lag_MarketValuePC = 0 then d.ReturnPC / d.lag_MarketValuePC
						else isnull(d.bm_ReturnPC / nullif(d.bm_lag_MarketValuePC, 0), 0.0)
					end - isnull(d.up_bm_ReturnPC / nullif(d.up_bm_lag_MarketValuePC, 0), 0.0)) as Allocation,
				-- selection, by default: (Rp - Rb) * Wb
				-- if Rp is not defined (market value does not exist) or Rb is not defined: 0 (will show as interaction instead of selection)
				-- if Wb = 0: use Wp instead of Wb
				case
					when d.lag_MarketValuePC = 0 or d.bm_lag_MarketValuePC = 0 then 0.0
					else (d.ReturnPC / d.lag_MarketValuePC - d.bm_ReturnPC / d.bm_lag_MarketValuePC)
						* case 
							when d.bm_lag_MarketValuePC = 0 or d.up_bm_lag_MarketValuePC = 0 then d.lag_Wgt
							else d.bm_lag_Wgt
						end 
					end as Selection,
				-- interaction, by default: (Rp - Rb) * (Wp - Wb)
				-- if Rp is not defined (market value does not exist): Cp (contribution to upper level performance)
				-- if Wb = 0: 0  (effect shown in selection)
				case
					when d.lag_MarketValuePC = 0 then d.ReturnPC / nullif(d.up_lag_MarketValuePC, 0)
					when d.bm_lag_MarketValuePC = 0 or d.up_bm_lag_MarketValuePC = 0 then 0.0 
					else (d.ReturnPC / d.lag_MarketValuePC - d.bm_ReturnPC / d.bm_lag_MarketValuePC) * (d.lag_Wgt - d.bm_lag_Wgt)
				end as Interaction
		) a
		
);