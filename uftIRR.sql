
CREATE OR ALTER function PerfTest.uftIRR(@from_date date, @to_date date) returns table as

--------------------------------------------------------------------------------------------------------------------------
-- This inline table function calculates internal rate of return (IRR).
--
-- Using PerfTest.vPEFundMarketValueAndCashFlowByCCY for cashflows, but any list of cash flows and dates can be used.
--
-- A recursive CTE will iterate to a suitable solution in all CalculationCurrencyType currencies.
-- Results are calculated for each Portfolio and Instrument separately, and for each Portfolio in total (total only in PortfolioCurrency).
--
-- Hard coded variables (could as well be given as function parameters, if necessary):
--     IRR initial value (guess) = 10 %
--     Maximum residual NPV = 0.01 (in currency)
--     Maximum iterations (loops) = 20
--
-- 2024-11-16   Kalle Saariaho     Initial release.
--------------------------------------------------------------------------------------------------------------------------

return (

with 

	-- define cash flows, using the prevailing market values to represent investment on @from_date and residual value on @to_date
	cashflows_instrument as (

		-- cash flows after @from date and on or before @to_date
		select
			PortfolioIK,
			PortfolioShortName,
			PortfolioLongName,
			PortfolioCurrency,
			InstrumentIK,
			ShortName,
			LongName,
			InstrumentCurrency,
			CalculationCurrencyType,
			CalculationCurrency,
			ToDate,
			convert(float, datediff(day, @from_date, ToDate)) / 365 as CashFlowTerm,
			-CashFlow as CashFlow,
			0 as IsEndValue
		from
			PerfTest.vPEFundMarketValueAndCashFlowByCCY
		where 
			ToDate > @from_date
			and ToDate <= @to_date
			and CashFlow <> 0 				

		-- prevailing market value (adjusted for cash flows) on @from_date as a negative cash flow (= starting value)
		union all
		select 
			a.PortfolioIK,
			a.PortfolioShortName,
			a.PortfolioLongName,
			a.PortfolioCurrency,
			a.InstrumentIK,
			a.ShortName,
			a.LongName,
			a.InstrumentCurrency,
			a.CalculationCurrencyType,
			a.CalculationCurrency,
			@from_date as ToDate,
			convert(float, 0) as CashFlowTerm,
			-(a.MarketValue + a.CashFlowAdjustment) as CashFlow,
			0 as IsEndValue
		from
			PerfTest.vPEFundMarketValueAndCashFlowByCCY a
			inner join (
				select PortfolioIK, InstrumentIK, CalculationCurrencyType, max(ToDate) as max_date 
				from PerfTest.vPEFundMarketValueAndCashFlowByCCY 
				where ToDate <= @from_date 
				group by PortfolioIK, InstrumentIK, CalculationCurrencyType
			) b	on b.PortfolioIK = a.PortfolioIK and b.InstrumentIK = a.InstrumentIK and b.CalculationCurrencyType = a.CalculationCurrencyType and b.max_date = a.ToDate 
		where
			a.CumUnits > 0  -- fund must be active on @from_date

		-- prevailing market value (adjusted for cash flows) on @to_date as a positive cash flow (= ending value)
		union all
		select 
			a.PortfolioIK,
			a.PortfolioShortName,
			a.PortfolioLongName,
			a.PortfolioCurrency,
			a.InstrumentIK,
			a.ShortName,
			a.LongName,
			a.InstrumentCurrency,
			a.CalculationCurrencyType,
			a.CalculationCurrency,
			c.ToDate,
			convert(float, datediff(day, @from_date, c.ToDate)) / 365 as CashFlowTerm,
			(a.MarketValue + a.CashFlowAdjustment) as CashFlow,
			1 as IsEndValue
		from
			PerfTest.vPEFundMarketValueAndCashFlowByCCY a
			inner join (
				select PortfolioIK, InstrumentIK, CalculationCurrencyType, max(ToDate) as max_date 
				from PerfTest.vPEFundMarketValueAndCashFlowByCCY 
				where ToDate <= @to_date
				group by PortfolioIK, InstrumentIK, CalculationCurrencyType
			) b	on b.PortfolioIK = a.PortfolioIK and b.InstrumentIK = a.InstrumentIK and b.CalculationCurrencyType = a.CalculationCurrencyType and b.max_date = a.ToDate 
			cross apply (
				select
					case
						when a.CumUnits > 0 then @to_date  -- fund still active on @to_date
						when a.ToDate <= @from_date then null  -- fund was closed on or before @from_date
						else a.ToDate  -- fund closed between @from_date and @to_date, use closing date
					end as ToDate
			) c
		where
			c.ToDate is not null  -- fund was closed on or before @from_date, exclude it

	),

	-- -- add Portfolio level. Accept PortfolioCurrency as the only currency type, others may not be using just one currency.
	cashflows as (

		select
			*
		from
			cashflows_instrument

		union all

		select
			PortfolioIK,
			PortfolioShortName,
			PortfolioLongName,
			PortfolioCurrency,
			-1 as InstrumentIK,
			'Total Portfolio' as ShortName,
			'Total Portfolio' as LongName,
			null as InstrumentCurrency,
			CalculationCurrencyType,
			CalculationCurrency,
			ToDate,
			CashFlowTerm,
			CashFlow,
			-- mark just one of the cashflows with IsEndValue = 1 on portfolio level.
			case
				when row_number() over (partition by PortfolioIK, CalculationCurrencyType order by ToDate) = count(PortfolioIK) over (partition by PortfolioIK, CalculationCurrencyType) then 1
				else 0
			end as IsEndValue
		from
			cashflows_instrument
		where
			CalculationCurrencyType = 'PortfolioCurrency'

	),

	-- define recursive tree, calculating Net Present Value of cash flows with each successive IRR estimate, and estimating the new IRR.
	iter_data as (

		-- anchor member, with a first guess as IRR. Grouping data by Portfolio, Instrument and CalculationCurrencyType.
		select
			cf.PortfolioIK,
			cf.PortfolioShortName,
			cf.PortfolioLongName,
			cf.PortfolioCurrency,
			cf.InstrumentIK,
			cf.ShortName,
			cf.LongName,
			cf.InstrumentCurrency,
			cf.CalculationCurrencyType,
			cf.CalculationCurrency,
			min(cf.ToDate) as MinDate,
			max(cf.ToDate) as MaxDate,
			1 as IsEndValue,
			convert(int, 0) as IterNr,  -- iteration counter
			convert(varchar(50), 'Starting') as IterStatus,
			convert(float, 0) as IRR_Previous,  -- 0 % as "previous IRR"
			convert(float, 0.1) as IRR,  -- starting from guess = 10 % IRR
			convert(float, sum(cf.CashFlow)) as NPV_Previous,
			convert(float, sum(cf.CashFlow / power(1e0 + 0.1, cf.CashFlowTerm))) as NPV  -- net present value with initial guess = 10 % IRR
		from
			cashflows cf
		group by
			cf.PortfolioIK,
			cf.PortfolioShortName,
			cf.PortfolioLongName,
			cf.PortfolioCurrency,
			cf.InstrumentIK,
			cf.ShortName,
			cf.LongName,
			cf.InstrumentCurrency,
			cf.CalculationCurrencyType,
			cf.CalculationCurrency

		union all

		-- recursive member, using iter_data itself in from clause.
		select

			iter_set.PortfolioIK,
			iter_set.PortfolioShortName,
			iter_set.PortfolioLongName,
			iter_set.PortfolioCurrency,
			iter_set.InstrumentIK,
			iter_set.ShortName,
			iter_set.LongName,
			iter_set.InstrumentCurrency,
			iter_set.CalculationCurrencyType,
			iter_set.CalculationCurrency,
			iter_set.MinDate,
			iter_set.MaxDate,
			iter_set.IsEndValue,
			iter_set.IterNr + 1 as IterNr,  -- increase iteration counter
			q.IterStatus,
			iter_set.IRR_Previous,
			iter_set.IRR,  -- the new IRR iteration result  
			iter_set.NPV_Previous,
			iter_set.NPV  -- present value with new IRR
		
		from (

			-- combining previous round IRR estimates with cash flows to calculate new IRRs and NPVs.
			-- recursive CTE cannot use group by, so using a window function to sum up cash flow present values, then in where clause picking just one row out of them, as many IRRs calculated simultaneously.
			select
				id.PortfolioIK,
				id.PortfolioShortName,
				id.PortfolioLongName,
				id.PortfolioCurrency,
				id.InstrumentIK,
				id.ShortName,
				id.LongName,
				id.InstrumentCurrency,
				id.CalculationCurrencyType,
				id.CalculationCurrency,
				id.MinDate,
				id.MaxDate,
				cf.IsEndValue,
				id.IterNr,
				id.IterStatus,
				id.IRR as IRR_Previous,
				n.IRR,  -- the new IRR iteration result  
				id.NPV as NPV_Previous,
				case 
					when n.IRR <= -0.99 then null
					else sum(convert(float, cf.CashFlow) / power(1e0 + n.IRR, cf.CashFlowTerm)) over (partition by id.PortfolioIK, id.InstrumentIK, id.CalculationCurrencyType)  -- use window function to sum over all cash flows
				end as NPV  -- net present value with new IRR

			from
				iter_data id
				inner join cashflows cf on cf.PortfolioIK = id.PortfolioIK and cf.InstrumentIK = id.InstrumentIK and cf.CalculationCurrencyType = id.CalculationCurrencyType

				cross apply (
					-- iterate towards the solution by estimating the new IRR
					select
						case 
							when id.NPV - id.NPV_Previous = 0 then id.IRR  -- no change in NPV, keep the result
							when id.IRR - (id.IRR - id.IRR_Previous) * id.NPV / (id.NPV - id.NPV_Previous) < -0.99 then -0.99  -- avoid <= -100 % IRR and the resulting error
							else id.IRR - (id.IRR - id.IRR_Previous) * id.NPV / (id.NPV - id.NPV_Previous)
						end as IRR
				) n

			where
				-- continue until result is final or an error status encountered
				id.IterStatus in ('Starting', 'Iterating')

			) iter_set

			cross apply (
				-- check if this was the final IRR - either accurate enough, maximum number of iterations reached, or no result
				select
					convert(varchar(50), case
						when abs(iter_set.NPV) <= 0.01 then 'Final'     -- break loop when NPV close enough to zero
						when iter_set.IterNr >= 20 then 'Error: max recursive loops'
						when iter_set.IRR > 100 then 'Error: IRR > 10,000 %'
						when iter_set.IRR <= -0.99 then 'Error: IRR <= -99 %'
						else 'Iterating'
					end) as IterStatus
			) q

		where
			-- pick only one row from cashflows, as new IRRs and NPVs on every row.
			iter_set.IsEndValue = 1

	)

	-- return results
	select 
		PortfolioIK,
		PortfolioShortName,
		PortfolioLongName,
		PortfolioCurrency,
		InstrumentIK,
		ShortName,
		LongName,
		InstrumentCurrency,
		CalculationCurrencyType,
		CalculationCurrency,
		MinDate,
		MaxDate,
		IterNr,
		IterStatus,
		case
			when IterStatus in ('Starting', 'Iterating') then 0
			else 1
		end as IsFinal,
		IRR,
		NPV,
		IRR_Previous,
		NPV_Previous
	from 
		iter_data
	
);
