create or alter function PerfTest.uftPerformanceAttributionCumulative(@portfolioik int, @bm_portfolioik int, @from_date date, @to_date date) returns table as

--------------------------------------------------------------------------------------------------------------------------
-- This inline table function calculates daily cumulative Brinson-Fachler performance attribution and performance contribution from daily data using Cariño scaling.
--
-- @from_date is the end of day of the calculation period start; i.e. for October data @from_date should be the last data date of September (to start cumulative series from zero).
--
-- 2023-12-16   Kalle Saariaho     Initial release
--------------------------------------------------------------------------------------------------------------------------

return (

	with items as (

		-- get a list of all items and their data starting date
		-- needed to make sure all time series continue from starting date to @to_date for daily sums of cumulative sums to work correctly
		select

			StructureIK,
			LevelCnt,
			LevelNr,
			Level1Name,
			Level1,
			Level2Name,
			Level2,
			Level3Name,
			Level3,
			Level4Name,
			Level4,
			min(Todate) as minDate

		from 

			PerfTest.uftPerformanceAttribution(@portfolioik, @bm_portfolioik)

		where 

			Todate between @from_date and @to_date

		group by

			StructureIK,
			LevelCnt,
			LevelNr,
			Level1Name,
			Level1,
			Level2Name,
			Level2,
			Level3Name,
			Level3,
			Level4Name,
			Level4

	), 

	dat as (

		-- add running cumulative scaled factors and also log returns needed for scaling back to percentage
		-- to daily data provided by PerfTest.uftPerformanceAttribution()
		select 

			i.StructureIK,
			i.LevelCnt,
			i.LevelNr,
			i.Level1Name,
			i.Level1,
			i.Level2Name,
			i.Level2,
			i.Level3Name,
			i.Level3,
			i.Level4Name,
			i.Level4,
			c.Todate,

			d.Wgt,
			d.bm_Wgt,
			f.ReturnPCPerc,
			f.bm_ReturnPCPerc,

			f.ContribPerc,
			f.bm_ContribPerc,
			f.Allocation,
			f.Selection,
			f.Interaction,

			-- window functions to calculate cumulative period factors, scaled to log returns
			sum(f.ContribPerc * s.ContribScaling)		over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as ContribPerc_Cumul,
			sum(f.bm_ContribPerc * s.bm_ContribScaling)	over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as bm_ContribPerc_Cumul,
			sum(f.Allocation * s.AttrScaling)			over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as Allocation_Cumul,
			sum(f.Selection * s.AttrScaling)			over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as Selection_Cumul,
			sum(f.Interaction * s.AttrScaling)			over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as Interaction_Cumul,
			-- window functions to calculate cumulative period Log returns, upper level needed for scaling the above factors back to match percentage returns
			sum(f.ReturnPCLog)							over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as ReturnPCLog_Cumul,
			sum(f.bm_ReturnPCLog)						over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as bm_ReturnPCLog_Cumul,
			sum(f.up_ReturnPCLog)						over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as up_ReturnPCLog_Cumul,
			sum(f.up_bm_ReturnPCLog)					over (partition by i.StructureIK, i.LevelNr, i.Level1, i.Level2, i.Level3, i.Level4 order by c.Todate rows between unbounded preceding and current row) as up_bm_ReturnPCLog_Cumul

		from 

			-- join all items to calendar so that we have all time series from their start date up to function @to_date
			items i
			inner join PerfTest.Calendar c on c.Todate >= i.minDate
			-- get actual attribution daily data for the dates found
			left outer join PerfTest.uftPerformanceAttribution(@portfolioik, @bm_portfolioik) d on d.Todate = c.ToDate
				and d.StructureIK = i.StructureIK 
				and d.LevelNr = i.LevelNr 
				and (d.Level1 = i.Level1 or (d.Level1 is null and i.Level1 is null))
				and (d.Level2 = i.Level2 or (d.Level2 is null and i.Level2 is null))
				and (d.Level3 = i.Level3 or (d.Level3 is null and i.Level3 is null))
				and (d.Level4 = i.Level4 or (d.Level4 is null and i.Level4 is null))
			-- if no data, we need upper level performance data for scaling use
			left outer join PerfTest.uftPerformanceTimeSeries(@portfolioik, @bm_portfolioik) t on t.Todate = c.ToDate and d.Todate is null
				and t.StructureIK = i.StructureIK 
				and (
					-- top level: use itself as upper node
					(t.LevelNr = 0 and i.LevelNr = 0) 
					-- other levels: upper level LevelNr = item LevelNr-1, and match all upper level identifiers
					or (
						i.LevelNr > 0
						and t.LevelNr = i.LevelNr - 1
						and (i.LevelNr <= 1 or t.Level1 = i.Level1)
						and (i.LevelNr <= 2 or t.Level2 = i.Level2)
						and (i.LevelNr <= 3 or t.Level3 = i.Level3)
					)
				)

			cross apply (
				-- to begin cumulative time series at zero, set return values to null on @from_date
				-- convert PC currency returns to float for accuracy in calculations
				-- upper level return and market value data: if no data found through the item, use separately joined time series data
				select
					case when d.Todate = @from_date then null else d.ReturnPCLog end as ReturnPCLog,
					case when d.Todate = @from_date then null else d.bm_ReturnPCLog end as bm_ReturnPCLog,
					case when d.Todate = @from_date then null else isnull(d.up_ReturnPCLog, t.ReturnPCLog) end as up_ReturnPCLog,
					case when d.Todate = @from_date then null else isnull(d.up_bm_ReturnPCLog, t.bm_ReturnPCLog) end as up_bm_ReturnPCLog,
					case when d.Todate = @from_date then null else d.ReturnPCPerc end as ReturnPCPerc,
					case when d.Todate = @from_date then null else d.bm_ReturnPCPerc end as bm_ReturnPCPerc,
					case when d.Todate = @from_date then null else d.ContribPerc end as ContribPerc,
					case when d.Todate = @from_date then null else d.bm_ContribPerc end as bm_ContribPerc,
					case when d.Todate = @from_date then null else d.Allocation end as Allocation,
					case when d.Todate = @from_date then null else d.Selection end as Selection,
					case when d.Todate = @from_date then null else d.Interaction end as Interaction,

					convert(float, coalesce(d.up_ReturnPC, t.ReturnPC, 0)) as up_ReturnPC,
					convert(float, coalesce(d.up_bm_ReturnPC, t.bm_ReturnPC, 0)) as up_bm_ReturnPC,
					convert(float, coalesce(d.up_lag_MarketValuePC, t.lag_MarketValuePC, 0)) as up_lag_MarketValuePC,
					convert(float, coalesce(d.up_bm_lag_MarketValuePC, t.bm_lag_MarketValuePC, 0)) as up_bm_lag_MarketValuePC
			) f

			cross apply (
				-- calculate Cariño scaling factors. Simply scaling the daily percentage factors to log factors --> sum of log factors will match the explained log returns. 
				-- R = return (perc)	r = return (log)	W = Weight	U = upper level	p = portfolio	b = benchmark
				select
					-- daily contribution scaling factors: by default rUp / RUp
					case
						when f.up_ReturnPC = 0 or f.up_lag_MarketValuePC = 0 then cast(1.0 as float)
						when f.up_ReturnPC / f.up_lag_MarketValuePC <= -1.0 then cast(1.0 as float)
						else log(1.0 + f.up_ReturnPC / f.up_lag_MarketValuePC) / (f.up_ReturnPC / f.up_lag_MarketValuePC)
					end as ContribScaling,
					case
						when f.up_bm_ReturnPC = 0 or f.up_bm_lag_MarketValuePC = 0 then cast(1.0 as float)
						when f.up_bm_ReturnPC / f.up_bm_lag_MarketValuePC <= -1.0 then cast(1.0 as float)
						else log(1.0 + f.up_bm_ReturnPC / f.up_bm_lag_MarketValuePC) / (f.up_bm_ReturnPC / f.up_bm_lag_MarketValuePC)
					end as bm_ContribScaling,

					-- daily attribution scaling factor: by default (rUp - rUb) / (RUp - RUb)
					-- if RUp = RUb: 1 / (1 + RUp)    (not using exactly equal but difference < 1.0E-12 - infinitesimal differences cause issues with division by zero)
					-- if no sensible data, 1
					case
						when f.up_lag_MarketValuePC = 0 then cast(1.0 as float)
						when f.up_bm_lag_MarketValuePC = 0 then cast(1.0 as float)
						when f.up_ReturnPC / f.up_lag_MarketValuePC <= -1.0 then cast(1.0 as float)
						when abs(f.up_ReturnPC / f.up_lag_MarketValuePC - f.up_bm_ReturnPC / f.up_bm_lag_MarketValuePC) < 1.0E-12 then cast(1.0 as float) / (1.0 + f.up_ReturnPC / f.up_lag_MarketValuePC)
						else (log(1.0 + f.up_ReturnPC / f.up_lag_MarketValuePC) - log(1.0 + f.up_bm_ReturnPC / f.up_bm_lag_MarketValuePC))
							/ (f.up_ReturnPC / f.up_lag_MarketValuePC - f.up_bm_ReturnPC / f.up_bm_lag_MarketValuePC)
					end as AttrScaling
			) s

		where 

			c.Todate <= @to_date

		)

	select

		-- calculate period scaling factors and add cumulative percentage returns and scaled cumulative contribution and attribution factors

		a.StructureIK,
		a.LevelCnt,
		a.LevelNr,
		a.Level1Name,
		a.Level1,
		a.Level2Name,
		a.Level2,
		a.Level3Name,
		a.Level3,
		a.Level4Name,
		a.Level4,
		a.Todate,

		a.Wgt,
		a.bm_Wgt,
		a.ReturnPCPerc,
		a.bm_ReturnPCPerc,
		a.ReturnPCPerc - bm_ReturnPCPerc as diff_ReturnPCPerc,

		a.ContribPerc,
		a.bm_ContribPerc,
		a.Allocation,
		a.Selection,
		a.Interaction,

		isnull(exp(a.up_ReturnPCLog_Cumul) - 1.0, 0.0) as up_ReturnPCPerc_Cumul,
		isnull(exp(a.up_bm_ReturnPCLog_Cumul) - 1.0, 0.0) as up_bm_ReturnPCPerc_Cumul,
		isnull(exp(a.up_ReturnPCLog_Cumul) - exp(a.up_bm_ReturnPCLog_Cumul), 0.0) as diff_up_ReturnPCPerc_Cumul,
		isnull(exp(a.ReturnPCLog_Cumul) - 1.0, 0.0) as ReturnPCPerc_Cumul,
		isnull(exp(a.bm_ReturnPCLog_Cumul) - 1.0, 0.0) as bm_ReturnPCPerc_Cumul,
		isnull(exp(a.ReturnPCLog_Cumul) - exp(a.bm_ReturnPCLog_Cumul), 0.0) as diff_ReturnPCPerc_Cumul,

		isnull(a.ContribPerc_Cumul		/ nullif(sp.ContribScalingPeriod, 0), 0.0) as ContribPerc_Cumul,
		isnull(a.bm_ContribPerc_Cumul	/ nullif(sp.bm_ContribScalingPeriod, 0), 0.0) as bm_ContribPerc_Cumul,
		isnull(a.Allocation_Cumul		/ nullif(sp.AttrScalingPeriod, 0), 0.0) as Allocation_Cumul,
		isnull(a.Selection_Cumul		/ nullif(sp.AttrScalingPeriod, 0), 0.0) as Selection_Cumul,
		isnull(a.Interaction_Cumul		/ nullif(sp.AttrScalingPeriod, 0), 0.0) as Interaction_Cumul

	from

		dat as a
		cross apply (
			-- Scale log factors back to percentage factors using period log and percentage returns.
			-- R = return (perc)	r = return (log)	W = Weight	U = upper level	p = portfolio	b = benchmark
			select
				-- contribution scaling factors for the period: by default rUp / RUp
				case
					when exp(a.up_ReturnPCLog_Cumul) - 1.0 = 0 then cast(1.0 as float)
					else a.up_ReturnPCLog_Cumul / (exp(a.up_ReturnPCLog_Cumul) - 1.0)
				end as ContribScalingPeriod,
				case
					when exp(a.up_bm_ReturnPCLog_Cumul) - 1.0 = 0 then cast(1.0 as float)
					else a.up_bm_ReturnPCLog_Cumul / (exp(a.up_bm_ReturnPCLog_Cumul) - 1.0)
				end as bm_ContribScalingPeriod,

				-- attribution scaling factor for the period: by default (rUp - rUb) / (RUp - RUb)
				-- if RUp = RUb: 1 / (1 + RUp)    (not using exactly equal but difference < 1.0E-12 - infinitesimal differences cause issues with division by zero)
				case
					when abs(a.up_ReturnPCLog_Cumul - a.up_bm_ReturnPCLog_Cumul) < 1.0E-12 then cast(1.0 as float) / exp(a.up_ReturnPCLog_Cumul)
					else (a.up_ReturnPCLog_Cumul - a.up_bm_ReturnPCLog_Cumul) / (exp(a.up_ReturnPCLog_Cumul) - exp(a.up_bm_ReturnPCLog_Cumul))
				end as AttrScalingPeriod
		) sp

);